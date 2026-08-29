"""The wire format.

    NSGEX2|202608031345Z|20260803|<record>|<record>|...

One line, ASCII, no whitespace. The indicator refuses anything it does not
recognise rather than misparsing it, so the encoder is strict and the decoder here
exists so a round-trip test can prove the blob parses back to the same numbers.

After a chunk's level tokens come its two wall tokens, `WC...` and `WP...`. They
are additive: they sit last in the chunk and open with a tag that is neither `C`
nor `P`, so a reader that only knows levels stops recognising them and skips them.
Nothing already on the wire moved, which is why this is still NSGEX2 -- and
skipping an unknown token rather than erroring on it is the rule the whole format
grows by.

A wall token carries an OPTIONAL third colon-separated field, the probability that
price touches that strike before today's close:

    WC7750:+4.15:41     three fields -- 41% chance of touching 7750 today
    WC7750:+4.15        two fields -- it could not be computed, and is omitted

Both shapes are valid and every reader must handle both. The field is never
emitted empty, as a placeholder, or as a fabricated zero.
"""

import re

from . import BLOB_PREFIX
from .nytime import date_stamp, utc_stamp
from .record import Level, Record, Section, Wall

FIELD_SEP = "|"
CHUNK_SEP = ";"
ITEM_SEP = ","
LEVEL_SEP = ":"
WALL_PREFIX = "W"

_STAMP_RE = re.compile(r"^\d{12}Z$")
_DATE_RE = re.compile(r"^\d{8}$")
_LEVEL_RE = re.compile(r"^([CP])(\d+(?:\.\d+)?):([+-]\d+\.\d{2})$")
# The third group is the OPTIONAL touch probability. Two fields and three are
# both valid, and the same blob can carry one wall with it and another without.
_WALL_RE = re.compile(r"^W([CP])(\d+(?:\.\d+)?):([+-]\d+\.\d{2})(?::(\d{1,3}))?$")


class BlobFormatError(ValueError):
    """The blob does not match the format the indicator accepts."""


def fmt_price(value):
    """Two decimals at most, trailing zeros trimmed: 730, not 730.00."""
    text = "%.3f" % value
    text = text.rstrip("0").rstrip(".")
    return text if text else "0"


def fmt_2dp(value):
    return "%.2f" % value


def fmt_signed(value):
    """Always sign-prefixed, 2dp. A value rounding to zero is '+0.00', never '-0.00'."""
    text = "%+.2f" % value
    return "+0.00" if text == "-0.00" else text


def fmt_flip(value):
    """2dp, or a bare '0' when there is no crossing."""
    return "0" if not value else fmt_2dp(value)


def encode_level(level):
    return "%s%s%s%s" % (level.right, fmt_price(level.price), LEVEL_SEP,
                         fmt_signed(level.magnitude))


def encode_wall(wall):
    """`WC7700:+14.55` -- the side, the strike, then the ONE-SIDED magnitude.

    An optional THIRD field follows when the touch probability could be computed:
    `WC7700:+14.55:41` means a 41% chance of price touching 7700 before today's
    close. When it could not, the token stays exactly two fields -- never an empty
    third field, never a placeholder, never `null`, never a fabricated `0`.
    """
    if wall.side not in ("C", "P"):
        raise BlobFormatError("wall side %r must be C or P" % (wall.side,))
    token = "%s%s%s%s%s" % (WALL_PREFIX, wall.side, fmt_price(wall.price), LEVEL_SEP,
                            fmt_signed(wall.magnitude))
    touch = getattr(wall, "touch", None)
    if touch is None:
        return token
    if not isinstance(touch, int) or isinstance(touch, bool) or not 0 <= touch <= 100:
        raise BlobFormatError("wall touch %r must be an integer 0-100" % (touch,))
    return "%s%s%d" % (token, LEVEL_SEP, touch)


def wall_tokens(section):
    """A chunk's wall tokens, call first, last in the chunk. Absent walls emit
    nothing -- a chunk with no gamma on one side carries one token, not a fake."""
    out = []
    if section.call_wall is not None:
        out.append(encode_wall(section.call_wall))
    if section.put_wall is not None:
        out.append(encode_wall(section.put_wall))
    return out


def encode_section(section):
    if section.tag is None:
        raise BlobFormatError("total chunk is encoded via encode_record")
    parts = [section.tag, fmt_flip(section.flip), fmt_signed(section.net)]
    parts.extend(encode_level(lv) for lv in section.levels)
    parts.extend(wall_tokens(section))
    return ITEM_SEP.join(parts)


def encode_record(record, ticker=None):
    total = record.total
    parts = [ticker or record.ticker, fmt_2dp(total.spot), fmt_flip(total.flip),
             fmt_signed(total.net)]
    parts.extend(encode_level(lv) for lv in total.levels)
    parts.extend(wall_tokens(total))
    chunks = [ITEM_SEP.join(parts)]
    chunks.extend(encode_section(s) for s in record.buckets)
    return CHUNK_SEP.join(chunks)


def encode_blob(records, computed_at, session_date):
    """Assemble the full line. Raises if anything non-ASCII or whitespace slips in."""
    if not records:
        raise BlobFormatError("refusing to encode an empty blob")
    body = FIELD_SEP.join(encode_record(r) for r in records)
    blob = FIELD_SEP.join([BLOB_PREFIX, utc_stamp(computed_at),
                           date_stamp(session_date), body])
    if any(ch.isspace() for ch in blob):
        raise BlobFormatError("blob contains whitespace")
    try:
        blob.encode("ascii")
    except UnicodeEncodeError:
        raise BlobFormatError("blob is not ASCII")
    return blob


# --- decoding, for the round-trip test and for eyeballing a saved blob ---------

def decode_level(text):
    match = _LEVEL_RE.match(text)
    if not match:
        raise BlobFormatError("bad level %r" % (text,))
    right, price, magnitude = match.groups()
    return Level(right=right, price=float(price), magnitude=float(magnitude))


def decode_wall(text):
    match = _WALL_RE.match(text)
    if not match:
        raise BlobFormatError("bad wall token %r" % (text,))
    side, price, magnitude, touch = match.groups()
    if touch is not None:
        touch = int(touch)
        if not 0 <= touch <= 100:
            raise BlobFormatError("wall touch %d out of range 0-100: %r"
                                  % (touch, text))
    return Wall(side=side, price=float(price), magnitude=float(magnitude),
                touch=touch)


def _decode_tokens(tokens):
    """Split a chunk's trailing tokens into levels and the two walls.

    Tokens are dispatched on their first character, not on their position, so a
    later addition to the format costs nothing here and nothing in the indicator:

      * `C` or `P` -- a level, as it always was.
      * `W` -- a wall.
      * anything else -- a token this reader does not know. **Skipped, never an
        error.** That is what lets tokens be added without breaking anything.

    A token whose tag *is* recognised but whose body will not parse still raises:
    that is our own encoder having produced something wrong, not a newer writer,
    and it must not pass quietly.
    """
    levels = []
    call_wall = None
    put_wall = None
    for token in tokens:
        if not token:
            continue
        head = token[0]
        if head in ("C", "P"):
            levels.append(decode_level(token))
        elif head == WALL_PREFIX:
            wall = decode_wall(token)
            if wall.side == "C":
                call_wall = wall
            else:
                put_wall = wall
    return levels, call_wall, put_wall


def _decode_flip(text):
    if text == "0":
        return 0.0
    try:
        return float(text)
    except ValueError:
        raise BlobFormatError("bad flip %r" % (text,))


def _decode_signed(text):
    if not text or text[0] not in "+-":
        raise BlobFormatError("value is not sign-prefixed: %r" % (text,))
    return float(text)


def decode_record(text):
    chunks = text.split(CHUNK_SEP)
    head = chunks[0].split(ITEM_SEP)
    if len(head) < 4:
        raise BlobFormatError("total chunk is too short: %r" % (chunks[0],))
    ticker, spot, flip, net = head[0], head[1], head[2], head[3]
    if not ticker or not ticker.isalnum():
        raise BlobFormatError("bad ticker %r" % (ticker,))
    levels, call_wall, put_wall = _decode_tokens(head[4:])
    sections = [Section(tag=None, spot=float(spot), flip=_decode_flip(flip),
                        net=_decode_signed(net), levels=levels,
                        call_wall=call_wall, put_wall=put_wall)]
    for chunk in chunks[1:]:
        items = chunk.split(ITEM_SEP)
        if len(items) < 3:
            raise BlobFormatError("bucket chunk is too short: %r" % (chunk,))
        tag = items[0]
        if tag not in ("0", "R"):
            raise BlobFormatError("unknown bucket tag %r" % (tag,))
        levels, call_wall, put_wall = _decode_tokens(items[3:])
        sections.append(Section(tag=tag, spot=None, flip=_decode_flip(items[1]),
                                net=_decode_signed(items[2]), levels=levels,
                                call_wall=call_wall, put_wall=put_wall))
    return Record(ticker=ticker, sections=sections)


def decode_blob(blob):
    """Return (computed_at_stamp, session_date_stamp, [Record, ...])."""
    if not isinstance(blob, str) or not blob:
        raise BlobFormatError("empty blob")
    fields = blob.split(FIELD_SEP)
    if len(fields) < 4:
        raise BlobFormatError("blob has no records")
    if fields[0] != BLOB_PREFIX:
        raise BlobFormatError("unknown version prefix %r" % (fields[0],))
    if not _STAMP_RE.match(fields[1]):
        raise BlobFormatError("bad computation stamp %r" % (fields[1],))
    if not _DATE_RE.match(fields[2]):
        raise BlobFormatError("bad session date %r" % (fields[2],))
    return fields[1], fields[2], [decode_record(f) for f in fields[3:]]
