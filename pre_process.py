import regex as re
import string
from copy import deepcopy
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

special_chars_list = ['→', '\u202a', '\uf0d8', '✤', '\u200c', 'ۣ', '🅖', '–', '₋', '●', '¬', '̶', '▬', '≈', '🫵', '◇', '▷', '🪷', '◊', '‐', '🫴', '\uf05b', '⦁', '️', '㎡', '🫰', '′', '✥', '✧', '♤', '🫶', 'ۜ', '❃', '̀', '֍', '\u2060', '\u206e', '‘', '❈', '🅣', '🅘', '℅', '\ufeff', '″', '\u200b', '♚', '̣', '₫', '\uf06e', '✩', '🅨', '’', '\xad', '★', '±', '\U0001fae8', '︎', '\uf0f0', '∙', '♛', '̉', '̛', '❆', '✜', '÷', '♜', '·', '❖', '】', '❁', '🫱', '・', '€', '☛', '“', '■', '\uf046', '￼', '�', '\u200d', '🫠', '\uf0e8', '⁃', '≥', '～', '➣', '́', '🪩', '̃', '\uf02b', '᪥', '🪺', '♧', '❂', '。', '♡', '，', '🪸', '：', '¥', '❝', '̂', '\U0001fa77', '\uf0a7', 'ৣ', '⚘', '➢', '⇔', '、', '－', '✆', '🫣', '⛫', '►', '̆', '✎', '❯', '《', '\uf076', '❮', '❀', '̵', '🥹', '❉', '̷', '\uf028', '✽', '«', '⇒', '➤', '\uf0e0', '\U0001faad', '♙', '\uf0fc', '【', '➥', '¤', '＆', '🛇', '\x7f', '）', '—', '”', '❞', '》', '☆', '×', '✞', '✿', '≤', '🅐', '√', '°', '✓', '¡', '…', '•', '»', '❊', '➦', '\u06dd', '\uf06c', '¸']

def remove_special_chars_uds(special_chars_list):
    return udf(lambda s: remove_special_chars(s, special_chars_list), returnType=StringType())

def remove_special_chars(input_string, special_chars_list, at_once=False):
    if not input_string:
        return None
    if at_once:
        special_chars_string = ''.join(special_chars_list)
        translator = str.maketrans('', '', special_chars_string)
        result = input_string.translate(translator)
    else:
        result = input_string
        for c in special_chars_list:
            result = result.replace(c, '')
    return result

@udf(returnType=StringType())
def remove_duplicate_punctuation_sequence(input_string):
    def remove_duplicate_sequence(text, target_char, max_length):
        pattern_1 = re.escape(target_char) + '{' + str(max_length) + ',}'
        pattern_2 = '(' + r'\s*' + re.escape(target_char) + ')' + '{' + str(max_length) + ',}'      

        text = re.sub(pattern_2, target_char, text)
        text = re.sub(pattern_1, target_char, text)
        return result
    
    if not input_string:
        return None
    
    result = input_string
    for punc in string.punctuation:
        if punc == '\\':
            continue
        max_length = 3 if punc == '.' else 1
        result = remove_duplicate_sequence(result, punc, max_length)
        
    return result