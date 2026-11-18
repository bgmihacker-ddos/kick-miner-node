import json
import os
from loguru import logger

current_language = "en"
language_data = {}

def load_language(lang_code):
    """Загрузить lang"""
    global current_language, language_data
    
    lang_file = f"lang/{lang_code}.lang"
    if not os.path.exists(lang_file):
        logger.warning(f"🔤 Localization file not found: {lang_file}, defaulting to English")
        lang_code = "en"
        lang_file = f"lang/{lang_code}.lang"
    
    try:
        with open(lang_file, 'r', encoding='utf-8') as f:
            language_data = json.load(f)
        current_language = lang_code
        logger.info(f"🔤 Language set: {lang_code}")
    except Exception as e:
        logger.error(f"❌ Error loading localization file {lang_file}: {str(e)}")
        # En default
        with open("lang/en.lang", 'r', encoding='utf-8') as f:
            language_data = json.load(f)
        current_language = "en"

def t(key, **kwargs):
    global language_data
    
    text = language_data.get(key, f"ОТСУТСТВУЕТ_КЛЮЧ:{key}")
    
    # Подставляем параметры
    for k, v in kwargs.items():
        text = text.replace(f"{{{k}}}", str(v))
    

    return text
