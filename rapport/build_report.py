import re
from html import escape
from pathlib import Path

from pypdf import PdfReader
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (Image, PageBreak, Paragraph, Preformatted,
                               SimpleDocTemplate, Spacer, Table, TableStyle)

root = Path(__file__).resolve().parent
for regular, bold in [
    (Path("C:/Windows/Fonts/arial.ttf"), Path("C:/Windows/Fonts/arialbd.ttf")),
    (Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"), Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf")),
]:
    if regular.exists() and bold.exists():
        pdfmetrics.registerFont(TTFont("Report", str(regular)))
        pdfmetrics.registerFont(TTFont("ReportBold", str(bold)))
        break
else:
    raise RuntimeError("Installer Arial ou DejaVu Sans pour rendre le rapport")

styles = getSampleStyleSheet()
styles.add(ParagraphStyle("Body", fontName="Report", fontSize=9.4, leading=13.1, spaceAfter=6))
styles.add(ParagraphStyle("TitleReport", fontName="ReportBold", fontSize=19, leading=24, textColor=colors.HexColor("#123e52"), spaceAfter=12))
styles.add(ParagraphStyle("Section", fontName="ReportBold", fontSize=12, leading=16, textColor=colors.HexColor("#123e52"), spaceBefore=8, spaceAfter=6))
styles.add(ParagraphStyle("Subsection", fontName="ReportBold", fontSize=10, leading=14, spaceBefore=5, spaceAfter=4))
styles.add(ParagraphStyle("CodeReport", fontName="Courier", fontSize=7.1, leading=9.2, spaceAfter=8, backColor=colors.HexColor("#f0f5f7"), borderPadding=7))


def inline(value):
    value = escape(value)
    value = re.sub(r"\*\*(.+?)\*\*", r'<font name="ReportBold">\1</font>', value)
    value = re.sub(r"`(.+?)`", r'<font name="Courier">\1</font>', value)
    return value


def footer(canvas, document):
    canvas.setStrokeColor(colors.HexColor("#bbccd3"))
    canvas.line(42, 37, A4[0] - 42, 37)
    canvas.setFont("Report", 7)
    canvas.setFillColor(colors.HexColor("#50626b"))
    canvas.drawString(42, 25, "SahlPay | Ecosysteme Big Data 2 | EMSI 2026-2027")
    canvas.drawRightString(A4[0] - 42, 25, str(document.page))


source = (root / "rapport.md").read_text(encoding="utf-8").splitlines()
story = []
i = 0
while i < len(source):
    line = source[i].strip()
    if line == "<!-- pagebreak -->":
        story.append(PageBreak())
    elif line.startswith("```"):
        block = []
        i += 1
        while i < len(source) and not source[i].startswith("```"):
            block.append(source[i])
            i += 1
        story.append(Preformatted("\n".join(block), styles["CodeReport"]))
    elif line.startswith("!["):
        match = re.match(r"!\[(.*?)\]\((.*?)\)", line)
        image_path = (root / match.group(2)).resolve()
        picture = Image(str(image_path))
        width, height = picture.imageWidth, picture.imageHeight
        picture.drawHeight = 230
        picture.drawWidth = width / height * picture.drawHeight
        story.extend([picture, Paragraph(inline(match.group(1)), styles["Body"])])
    elif line.startswith("|"):
        rows = []
        while i < len(source) and source[i].strip().startswith("|"):
            cells = [cell.strip() for cell in source[i].strip().strip("|").split("|")]
            if not all(re.fullmatch(r"[-: ]+", cell) for cell in cells):
                rows.append([Paragraph(inline(cell), styles["Body"]) for cell in cells])
            i += 1
        table = Table(rows, colWidths=[(A4[0] - 84) / len(rows[0])] * len(rows[0]), hAlign="LEFT", repeatRows=1)
        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e4eef2")),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LINEBELOW", (0, 0), (-1, -1), .3, colors.HexColor("#cbd9df")),
            ("LEFTPADDING", (0, 0), (-1, -1), 6), ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 4), ("BOTTOMPADDING", (0, 0), (-1, -1), 2)]))
        story.extend([table, Spacer(1, 8)])
        continue
    elif line.startswith("### "):
        story.append(Paragraph(inline(line[4:]), styles["Subsection"]))
    elif line.startswith("## "):
        story.append(Paragraph(inline(line[3:]), styles["Section"]))
    elif line.startswith("# "):
        story.append(Paragraph(inline(line[2:]), styles["TitleReport"]))
    elif line:
        story.append(Paragraph(inline(line[2:] if line.startswith("- ") else line), styles["Body"], bulletText="-" if line.startswith("- ") else None))
    i += 1

output = root / "rapport_sahlpay.pdf"
document = SimpleDocTemplate(str(output), pagesize=A4, rightMargin=42, leftMargin=42,
                            topMargin=38, bottomMargin=49, title="SahlPay - Ecosysteme Big Data 2",
                            author="Projet EMSI - 5e annee")
document.build(story, onFirstPage=footer, onLaterPages=footer)
pages = len(PdfReader(output).pages)
if pages > 10:
    raise RuntimeError(f"Rapport trop long : {pages} pages")
print(f"{output}: {pages} pages")
