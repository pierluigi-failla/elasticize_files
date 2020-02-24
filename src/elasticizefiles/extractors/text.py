# -*- coding: utf-8 -*-
"""
Created by Pierluigi on 2020-02-24
project: elasticizefiles
"""

from elasticizefiles.base import Extractor


class ExtractText(Extractor):
    """ Extract text from several filetypes (.csv .doc .docx .eml .epub
    .gif .jpg .jpeg .json .html .htm .mp3 .msg .odt .ogg .pdf .png .pptx
    .ps .rtf .tiff .tif .txt .wav .xlsx .xls) as supported by `textract`
    (https://github.com/deanmalmgren/textract)

    """

    def __init__(self):
        Extractor.__init__(self)

    def extract(self, filename):
        try:
            import textract
        except Exception as e:
            raise Exception('module `textract` is not installed, try `pip install -U textract`')

        text = textract.process(filename)
        return {'text': text}

    def mapping(self):
        return {
            'text': {
                'type': 'text',
                'fields': {
                    'keywords': {
                        'type': 'keyword',
                    }
                }
            }
        }