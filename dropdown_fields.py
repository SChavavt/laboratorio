"""Catálogos compartidos entre tabla, ficha y alta; los adornos no se guardan."""
import hashlib
import re
from functools import lru_cache
from pathlib import Path

from st_aggrid import JsCode


def split_values(value, options=()):
    text = str(value or "").strip()
    if not text:
        return []
    native = {str(option).strip(): option for option in options if str(option).strip()}
    if text in native:
        return [native[text]]

    @lru_cache(maxsize=None)
    def parse(rest):
        if not rest:
            return ()
        for option in sorted(native, key=len, reverse=True):
            if rest == option:
                return (native[option],)
            if rest.startswith(option):
                tail = rest[len(option):]
                separator = re.match(r"\s*(?:,|\n)\s*", tail)
                if separator:
                    following = parse(tail[separator.end():])
                    if following is not None:
                        return (native[option], *following)
        return None

    parsed = parse(text)
    values = parsed if parsed is not None else [native.get(v.strip(), v.strip()) for v in re.split(r",\s*|\n", text) if v.strip()]
    return list(dict.fromkeys(values))


def join_values(values):
    return ", ".join(dict.fromkeys(values))


def valid_value(value, options, *, multiple=False, previous=""):
    values = split_values(value, options) if multiple else ([str(value)] if str(value or "").strip() else [])
    allowed = {str(option).strip() for option in options}
    # Permite conservar valores antiguos al añadir/quitar otro chip, sin ofrecer
    # valores arbitrarios nuevos ni forzar a borrar información del pedido.
    if multiple:
        allowed.update(str(option).strip() for option in split_values(previous, options))
    return all(str(option).strip() in allowed for option in values)


def multiple_columns(catalog, source=None, known=()):
    result = set(known) & set(catalog)
    if source is not None:
        for column, options in catalog.items():
            if column not in source:
                continue
            for value in source[column].dropna().unique():
                parts = split_values(value, options)
                if len(parts) > 1 and valid_value(value, options, multiple=True):
                    result.add(column)
                    break
    return result


TONES = [('#DBEAFE', '#1E40AF'), ('#DCFCE7', '#166534'), ('#F3E8FF', '#6B21A8'),
         ('#FFEDD5', '#9A3412'), ('#FCE7F3', '#9D174D'), ('#CFFAFE', '#155E75'),
         ('#FEF3C7', '#92400E'), ('#E0E7FF', '#3730A3')]


def option_colors(column, value, palettes=None):
    known = (palettes or {}).get(column, {})
    text = str(value).strip()
    if value in known or text in known:
        return known.get(value, known.get(text))
    index = int(hashlib.sha256(f'{column}:{text}'.encode()).hexdigest()[:8], 16) % len(TONES)
    return TONES[index]


@lru_cache(maxsize=2)
def dropdown_js(kind):
    root = Path(__file__).parent / 'ui'
    return JsCode((root / f'dropdown_{kind}.js').read_text().replace(
        'SPLIT_DROPDOWN_VALUES', '(' + (root / 'dropdown_values.js').read_text() + ')'))


def decorate_grid(options, catalog, source, *, multiple=(), labeler=lambda c, v: v, palettes=None,
                  canonicalize=lambda c, v: str(v)):
    specs = {}
    for column, choices in catalog.items():
        if column in {'STATUS', 'APARATO'}:
            continue  # Sus editores validan flujos específicos por pedido.
        values = list(choices)
        if column in source:
            for raw in source[column].dropna().unique():
                raw = canonicalize(column, raw)
                values.extend(split_values(raw, choices) if column in multiple else [str(raw)])
        values = list(dict.fromkeys(value for value in values if str(value).strip()))
        specs[column] = {'values': list(dict.fromkeys(v for v in choices if str(v).strip())), 'multiple': column in multiple,
            'labels': {value: labeler(column, value) for value in values},
            'aliases': {str(value): canonicalize(column, value) for value in source[column].dropna().unique()} if column in source else {},
            'colors': {value: option_colors(column, value, palettes) for value in values}}
    options['context']['dropdowns'] = specs
    for config in options['columnDefs']:
        column = config['field']
        if column not in specs:
            continue
        config['cellRenderer'] = dropdown_js('renderer')
        config['cellStyle'] = {'display': 'flex', 'alignItems': 'center'}
        if column in multiple:
            config.update(width=290, maxWidth=460, wrapText=True, autoHeight=True)
        if config.get('editable'):
            config.update(cellEditor=dropdown_js('editor'), cellEditorPopup=True)
            config.pop('cellEditorParams', None)
    return options
