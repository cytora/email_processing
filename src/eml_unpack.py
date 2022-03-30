
import dataclasses
import email
import re
from typing import Callable, Optional

import bs4  # type: ignore
import fitz
import pdfkit  # type: ignore
import structlog

logger = structlog.getLogger(__name__)


Mail = email.message.EmailMessage


@dataclasses.dataclass(repr=False)
class Attachment:
    name: str
    content_type: str
    content: bytes


class EMLExtractor:
    def __init__(self, mail: Mail, filename: Optional[str] = None):
        self.mail: Mail = mail

        #
        self.email_labels = self.mail['X-Gmail-Labels']
        self.email_date = self.mail['Date']
        self.email_from = self.mail['From']
        self.email_to = self.mail['To']
        self.email_subject = self.mail['Subject']
        #

        self.body_document: Optional[Attachment] = None
        self.documents: list[Attachment] = []
        self.html: Optional[bs4.BeautifulSoup] = None  # decoded to bytes in case of quoted-printable
        self.plain_text: Optional[str] = None
        self.filename: Optional[str] = filename

        self._nested_body_documents: list[Attachment] = []
        self._images: list[Mail] = []
        self._images_found_in_html: set[Mail] = set()
        self._methods_dict: dict[str, Callable[[Mail], None]] = self._create_methods_dict()

        if filename:
            logger.info(f"Extracting {filename}")

        self._parse_mail(mail)
        self._generate_body_doc()
        self._concatenate_body_docs()
        self._put_unused_images_in_documents()

    def _create_methods_dict(self) -> dict[str, Callable[[Mail], None]]:
        # a sprinkle of magic
        dict_ = {}
        for name in dir(self):
            if name.startswith("_EMLExtractor__"):
                method_name = name.replace("_EMLExtractor__", "")
                content_name = method_name.replace("_", "/", 1).replace("_", "-")
                dict_[content_name] = getattr(self, name)
        return dict_

    def _parse_mail(self, mail: Mail) -> None:
        logger.info(f"Parsing {mail.get_content_type()}")

        content_type = mail.get_content_type()
        content_maintype = mail.get_content_maintype()

        if content_type in self._methods_dict:
            method = self._methods_dict[content_type]
        elif content_maintype in self._methods_dict:
            method = self._methods_dict[content_maintype]
        else:
            method = self.__default

        method(mail)

    def _generate_body_doc(self) -> None:
        if not self.html:
            return self._generate_plaintext_body_doc()  # fallback to plaintext

        logger.info("Generating pdf from html")

        body = self.html

        # replace img tags with byte forms
        img_tags = body("img")  # somebody put a background image on the body, not handling it crashes pdfkit

        # sometimes the inline images get transformed into es$correspondimage001.jpg attachments...
        corresponding_images = [img for img in self._images if img.get_filename().startswith("es$correspond_image")]
        if corresponding_images and len(corresponding_images) == len(img_tags):
            logger.info("Images found as es$correspond_image attachments")
            corresponding_images = sorted(
                corresponding_images, key=lambda x: int(re.sub("[^0-9]", "", x.get_filename()))
            )
            for mail, img_tag in zip(corresponding_images, img_tags):
                img_type = mail.get_content_type()
                img_bytes = mail.get_payload()
                img_tag.attrs["src"] = f"data:{img_type};base64,{img_bytes}"
                self._images_found_in_html.add(mail)
            img_tags = []

        img_tags += body("body")  # somebody put a background image on the body, not handling it crashes pdfkit

        for img_tag in img_tags:
            # determine img tag attribute
            if "src" in img_tag.attrs:
                attr_name = "src"
            elif "background" in img_tag.attrs:
                attr_name = "background"
            else:
                continue  # no image source found, this'll happen on body tag

            # get img mail
            cid = img_tag.attrs[attr_name]
            img_mail = self._find_image_for_cid(cid)

            if img_mail:
                # substitute cid for img
                img_type = img_mail.get_content_type()
                img_bytes = img_mail.get_payload()
                img_tag.attrs[attr_name] = f"data:{img_type};base64,{img_bytes}"
            elif "http" not in img_tag.attrs[attr_name]:  # some weird links can crash pdfkit
                # remove non-http links
                logger.info(f"Removing img tag {img_tag}")
                img_tag.attrs[attr_name] = ""

        # HTML document response
        #self.body_document = Attachment(
        #    name="email_body.html",
        #    content_type="text",
        #    content=str(body),
        #)

        # add email header to the html top
        br = body.new_tag("br")
        body.html.body.insert(0, br)

        hr = body.new_tag("hr")
        body.html.body.insert(0, hr)

        _date = body.new_tag("p")
        _date = self.email_date
        body.html.body.insert(0, _date)

        _subject = body.new_tag("p")
        _subject = self.email_subject
        body.html.body.insert(0, _subject)

        _to = body.new_tag("p")
        _to = self.email_to
        body.html.body.insert(0, _to)

        _from = body.new_tag("p")
        _from = self.email_from
        body.html.body.insert(0, _from)

        _labels = body.new_tag("p")
        _labels = self.email_labels
        body.html.body.insert(0, _labels)

        header = body.new_tag("header")
        header.string = self.filename
        body.html.body.insert(0, header)



        # convert to pdf
        try:
            pdf = pdfkit.from_string(str(body))
        except OSError as e:
            logger.error("pdfkit crashed :(", exc_info=e)
            self._images_found_in_html = set()  # reset html replaced images
            return self._generate_plaintext_body_doc()  # fallback to plaintext

        self.body_document = Attachment(
            name="email_body.pdf",
            content_type="application/pdf",
            content=pdf,
        )

    def _generate_plaintext_body_doc(self) -> None:
        if not self.plain_text:
            return

        logger.info("Generating pdf from plain text")

        # TODO make plain text render prettier, unicode characters aren't rendered properly
        html = "<p>" + self.plain_text.replace("\n", "<br>") + "</p>"
        body = bs4.BeautifulSoup(html, "html.parser")
        try:
            pdf = pdfkit.from_string(str(body))
        except OSError as e:
            logger.error("pdfkit crashed on plain text O.o", exc_info=e)
            return

        self.body_document = Attachment(
            name="email_body.pdf",
            content_type="application/pdf",
            content=pdf,
        )

    def _concatenate_body_docs(self):
        if self._nested_body_documents:
            logger.info("Merging body document with nested emails'")
            main_doc = fitz.open(self.body_document.name, self.body_document.content)
            for nested_body_document in self._nested_body_documents:
                other_doc = fitz.open(nested_body_document.name, nested_body_document.content)
                main_doc.insert_pdf(other_doc)
            self.body_document = Attachment(
                name="mailbody.pdf",
                content_type="application/pdf",
                content=main_doc.tobytes(),
            )

    def _put_unused_images_in_documents(self) -> None:
        for mail in self._images:
            if (
                mail not in self._images_found_in_html
                and mail.is_attachment()
                and mail.get_content_disposition() != "inline"
                # TODO remove imagemagick requirement
                # and not self._similar_image_is_embedded(mail)
            ):
                logger.info(f"{mail.get_filename()} not found in html, adding to documents")
                self.__application(mail)

    def _similar_image_is_embedded(self, image_mail: Mail) -> bool:
        from wand.image import Image  # type: ignore

        # TODO this should optimize for detecting the same image at a different compression level
        # maybe compare DCT?
        image_bytes = image_mail.get_payload(decode=True)
        image = Image(blob=image_bytes)

        for embedded_image_mail in self._images_found_in_html:
            embedded_image_bytes = embedded_image_mail.get_payload(decode=True)
            embedded_image = Image(blob=embedded_image_bytes)

            diff = image.compare(embedded_image, metric="perceptual_hash")[1]

            if diff < 50:
                logger.info(
                    f"Dropping {image_mail.get_filename()}; "
                    f"found similar-looking {embedded_image_mail.get_filename()} embedded in mail"
                )
                return True

        return False

    def _find_image_for_cid(self, tag_cid: str) -> Optional[Mail]:
        def image_matches_tag(mail: Mail) -> bool:
            if tag_cid.startswith("cid:") and "Content-ID" in mail:
                cid = mail["Content-ID"]
                cid = cid.strip("<").strip(">")
                if cid == tag_cid[4:]:
                    return True
            if mail.get_filename() in tag_cid:
                logger.info("Didn't match image by CID, but did it by filename anyway")
                return True
            return False

        iterator = (mail for mail in self._images if image_matches_tag(mail))
        try:
            mail = next(iterator)
            self._images_found_in_html.add(mail)
            return mail
        except StopIteration:
            return None

    def __multipart(self, mail: Mail) -> None:
        for child_mail in mail.get_payload():
            self._parse_mail(child_mail)

    def __text_html(self, mail: Mail) -> None:
        # TODO concat several html parts
        bytes_ = mail.get_payload(decode=True)
        self.html = bs4.BeautifulSoup(bytes_, "html.parser")

    def __text_plain(self, mail: Mail) -> None:
        # TODO concat several plain text parts
        # also self.plain_text isn't used anywhere yet
        charset = "utf-8"
        if mail.get_charset():
            charset = str(mail.get_charset())
        elif "Content-Type" in mail:
            header_content_type = mail["Content-Type"]
            elements = header_content_type.split(";")
            for e in elements:
                e = e.strip()
                if e.startswith("charset"):
                    charset = e.split("=")[1].strip('"')
                    break
        try:
            self.plain_text = mail.get_payload(decode=True).decode(charset)
        except UnicodeDecodeError:
            logger.warning(f"Failed to decode plain text, tried using {charset}")

    def __image(self, mail: Mail) -> None:
        self._images.append(mail)

    def __application(self, mail: Mail) -> None:
        payload_bytes = mail.get_payload(decode=True)
        if not isinstance(payload_bytes, bytes):
            logger.debug(f"Unexpected attachment payload type: {payload_bytes}")
            return

        self.documents.append(
            Attachment(
                name=mail.get_filename(),
                content_type=mail.get_content_type(),
                content=payload_bytes,
            )
        )

    def __message_rfc822(self, mail: Mail) -> None:
        # this is a nested email

        # TODO check if this always has only one element?
        for part in mail.get_payload():
            extractor = EMLExtractor(part)
            self.documents.extend(extractor.documents)
            if extractor.body_document:
                self._nested_body_documents.append(extractor.body_document)

        # alternatively, you could just call self._parse_mail(mail) on the nested mail
        # if you do so, make sure to adjust how the html and plain text are concatenated
        # this may lead to namespace collisions for inline images though, so maybe creating a new extractor is better

    def __default(self, mail: Mail) -> None:
        logger.debug(f"Unexpected mail mimetype: {mail.get_content_type()}")
        # self.__multipart_related(mail)


if __name__ == "__main__":
    from email import policy
    from email.parser import BytesParser

    fn = "/Users/todorlubenov/cytora_data/bulk_datas/test/176. EXTERNAL Motor Fleet Enquiry for Bakro International Transport Ltd - Haulage Fleet and cars.msg.eml"

    with open(fn, "rb") as f:
        msg = BytesParser(policy=policy.default).parse(f)
    extractor = EMLExtractor(msg, fn.split("/")[-1])

    attachments = extractor.documents
    if extractor.body_document:
        attachments.append(extractor.body_document)

    import os

    dirname = f'/Users/todorlubenov/cytora_data/bulk_datas/test/my_pdf_renders/{fn.split("/")[-1]}'
    os.makedirs(dirname, exist_ok=True)
    for attachment in attachments:
        with open(f"{dirname}/{attachment.name}", "wb") as f:
            f.write(attachment.content)


# if __name__ == "__main__":
#     from email import policy
#     from email.parser import BytesParser
#     fn = "/Users/rafael/Downloads/sample_1_50 2/d62b000f-147a-4679-ae68-7c34ad78e5b7.eml"
#     # fn = '/Users/rafael/rs-component-pocs/beazley-source-emails-sample-2/5f3e5bd9-139e-4b1a-a236-6f31c9a58265.eml'
#     # for fn in glob.glob('/Users/rafael/Downloads/sample_1_50 2/*.eml'):
#     with open(fn, "rb") as f:
#         msg = BytesParser(policy=policy.default).parse(f)
#     extractor = EMLExtractor(msg, fn.split("/")[-1])
#
#     attachments = extractor.documents
#     if extractor.body_document:
#         attachments.append(extractor.body_document)
#     import os
#
#     dirname = f'/Users/rafael/Downloads/sample_1_50 2/my_pdf_renders/{fn.split("/")[-1]}'
#     os.makedirs(dirname, exist_ok=True)
#     for attachment in attachments:
#         with open(f"{dirname}/{attachment.name}", "wb") as f:
#             f.write(attachment.content)
