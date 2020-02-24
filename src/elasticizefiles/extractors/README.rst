==========
Extractors
==========

metadata.py
-----------

*ExtractExif*: based on module `hachoir` (https://github.com/vstinner/hachoir) allows to extract metadata from files (take a look to: https://hachoir.readthedocs.io/en/latest/metadata.html), particularly:

- Archives: bzip2, gzip, zip, tar
- Audio: MPEG audio ("MP3"), WAV, Sun/NeXT audio, Ogg/Vorbis (OGG), MIDI, AIFF, AIFC, Real audio (RA)
- Image: BMP, CUR, EMF, ICO, GIF, JPEG, PCX, PNG, TGA, TIFF, WMF, XCF
- Misc: Torrent
- Program: EXE
- Video: ASF format (WMV video), AVI, Matroska (MKV), Quicktime (MOV), Ogg/Theora, Real media (RM).

simple.py
---------

Simple extractors you can use as samples of writing extractors.

*ExtractFilename*: just extract the filename.

*ExtractSha256*: compute the SHA256 of binary file.

*ExtractPythonFunction*: extract function and class from python code storing with their line numbers.

text.py
-------

*ExtractText*: based on module `textract` (https://github.com/deanmalmgren/textract) allows to extract text from several different types of files: .csv .doc .docx .eml .epub .gif .jpg .jpeg .json .html .htm .mp3 .msg .odt .ogg .pdf .png .pptx .ps .rtf .tiff .tif .txt .wav .xlsx .xls