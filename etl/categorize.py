"""Sin la API de catalogo (no se implemento, ver Decisiones_Tecnicas.md sección 4):
la categoria de cada producto se infiere buscando palabras clave en su
descripcion. El nombre canonico de un producto tambien se resuelve aqui."""

import json
import logging

logger = logging.getLogger("datamart_etl")

DEFAULT_CATEGORY = "Sin clasificar"

# Usado solo si no se puede leer el archivo apuntado por la Airflow Variable
# category_keywords_path (ver etl/category_keywords.json).
DEFAULT_KEYWORDS = {
    "Papeleria": [
        "CARD", "NOTEBOOK", "PENCIL", "ENVELOPE", "STICKER", "WRAP", "PAPER",
        "STATIONERY", "POSTCARD", "BOOKMARK", "GIFT TAG",
    ],
    "Ropa": ["T-SHIRT", "SCARF", "HAT", "GLOVE", "SOCK", "APRON"],
    "Deportes": ["BALL", "GAME", "SPORT", "BICYCLE", "SKATE", "YOGA"],
    "Electronica": ["RADIO", "CAMERA", "BATTERY", "CABLE", "USB", "PHONE"],
    "Hogar": [
        "CANDLE", "HOLDER", "LANTERN", "MUG", "CUP", "JUG", "PLATE", "BOWL",
        "TIN", "BOX", "LIGHT", "CLOCK", "FRAME", "DOORMAT", "CUSHION", "SIGN",
        "DECORATION", "VASE", "BASKET", "HOOK", "MIRROR", "RUG", "TOWEL",
        "KITCHEN", "GARDEN", "PLANT", "CHRISTMAS", "HEART",
    ],
}


def load_category_keywords(path):
    """Diccionario categoría -> palabras clave, parametrizado vía la Airflow Variable
    category_keywords_path para poder ajustarlo sin tocar código (ver Decisiones_Tecnicas.md)."""
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        logger.warning("No se pudo leer %s, se usa el diccionario de categorías por defecto", path)
        return DEFAULT_KEYWORDS


def categorize(description, keywords):
    """Devuelve la primera categoría cuyas palabras clave aparezcan en la
    descripción (orden del diccionario = prioridad: las más específicas van
    primero en category_keywords.json). Sin coincidencias -> 'Sin clasificar'."""
    if not description:
        return DEFAULT_CATEGORY
    text = str(description).upper()
    for category, terms in keywords.items():
        if any(term in text for term in terms):
            return category
    return DEFAULT_CATEGORY


def pick_canonical_descriptions(df):
    """Recibe un DataFrame con columnas stock_code/description y devuelve, por
    stock_code, la descripción más frecuente (normalizada a mayúsculas). Empates
    de frecuencia se resuelven alfabéticamente para que el resultado sea determinístico."""
    normalized = df[["stock_code", "description"]].copy()
    normalized["description"] = normalized["description"].fillna("").str.strip().str.upper()
    normalized = normalized[normalized["description"] != ""]

    # Cuenta cuantas veces aparece cada variante de descripcion por stock_code,
    # y ordena por frecuencia descendente (mas frecuente primero) con la
    # descripcion como segundo criterio (alfabetico) para desempatar.
    counts = (
        normalized.groupby(["stock_code", "description"])
        .size()
        .reset_index(name="freq")
        .sort_values(["stock_code", "freq", "description"], ascending=[True, False, True])
    )
    # .first() por stock_code se queda con la primera fila de cada grupo segun
    # el orden de arriba: la mas frecuente (o la alfabeticamente primera si hay empate).
    canonical = counts.groupby("stock_code").first().reset_index()
    return canonical[["stock_code", "description"]].rename(columns={"description": "canonical_description"})
