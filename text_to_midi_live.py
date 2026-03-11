import time
import re
import random
import unicodedata
from pathlib import Path
from functools import lru_cache

try:
    import mido
    from mido import Message, MidiFile, MidiTrack, MetaMessage
except Exception:
    mido = None
    Message = None
    MidiFile = None
    MidiTrack = None
    MetaMessage = None

ARP_STEP_BASE_SEC = 0.022
REFERENCE_TEMPO_BPM = 120.0
DEFAULT_LOW_NOTE = 40
DEFAULT_HIGH_NOTE = 90
MIN_LOW_INTERVAL = 7
LOW_INTERVAL_PIVOT = 58
MAX_PITCH_BEND = 8191
OUTPUT_DIR = Path(__file__).resolve().parent / "midi_outputs"


def require_mido():
    if mido is None:
        raise RuntimeError("MIDI support is unavailable on this server (mido backend not installed).")

NOTE_NAME_TO_PC = {
    "C": 0,
    "B#": 0,
    "C#": 1,
    "DB": 1,
    "D": 2,
    "D#": 3,
    "EB": 3,
    "E": 4,
    "FB": 4,
    "F": 5,
    "E#": 5,
    "F#": 6,
    "GB": 6,
    "G": 7,
    "G#": 8,
    "AB": 8,
    "A": 9,
    "A#": 10,
    "BB": 10,
    "B": 11,
    "CB": 11,
}
PC_TO_NOTE_NAME = {0: "C", 1: "C#", 2: "D", 3: "Eb", 4: "E", 5: "F", 6: "F#", 7: "G", 8: "Ab", 9: "A", 10: "Bb", 11: "B"}

MODE_INTERVALS = {
    "major": [0, 2, 4, 5, 7, 9, 11],
    "minor": [0, 2, 3, 5, 7, 8, 10],
    "chromatic": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
    "major7": [0, 4, 7, 11],
    "minor7": [0, 3, 7, 10],
    "major_pentatonic": [0, 2, 4, 7, 9],
    "minor_pentatonic": [0, 3, 5, 7, 10],
    "dorian": [0, 2, 3, 5, 7, 9, 10],
    "phrygian": [0, 1, 3, 5, 7, 8, 10],
    "lydian": [0, 2, 4, 6, 7, 9, 11],
    "mixolydian": [0, 2, 4, 5, 7, 9, 10],
    "locrian": [0, 1, 3, 5, 6, 8, 10],
}

try:
    import pronouncing
except ImportError:
    pronouncing = None

WORD_TOKEN_RE = re.compile(r"[^\W\d_']+(?:['’][^\W\d_']+)*|\d+|[.,!?;:]", flags=re.UNICODE)
WORD_ONLY_RE = re.compile(r"[^\W\d_']+(?:['’][^\W\d_']+)*|\d+", flags=re.UNICODE)

# High-confidence overrides for "music" variants used in multilingual demos.
SPECIAL_SYLLABLE_OVERRIDES = {
    "music": 2,      # EN
    "musique": 2,    # FR
    "musica": 3,     # ES/IT/PT (from música/musica)
    "musik": 2,      # DE/ID
    "muziek": 2,     # NL
}
SPECIAL_UNIT_OVERRIDES = {
    "music": ["mu", "sic"],
    "musique": ["mu", "sique"],
    "musica": ["mu", "si", "ca"],
    "musik": ["mu", "sik"],
    "muziek": ["mu", "ziek"],
}


def normalized_word_ascii(word):
    if not word:
        return ""
    w = unicodedata.normalize("NFKD", str(word)).casefold()
    w = "".join(ch for ch in w if unicodedata.category(ch) != "Mn")
    return "".join(ch for ch in w if ch.isalpha() or ch == "'")


def word_key(word):
    return normalized_word_ascii(word).replace("'", "")


@lru_cache(maxsize=4096)
def count_syllables(word):
    key = word_key(word)
    if key in SPECIAL_SYLLABLE_OVERRIDES:
        return SPECIAL_SYLLABLE_OVERRIDES[key]

    w = re.sub(r"[^a-z]", "", normalized_word_ascii(word))
    if not w:
        return 1

    if pronouncing is not None:
        phones = pronouncing.phones_for_word(w)
        if phones:
            counts = [pronouncing.syllable_count(p) for p in phones]
            valid_counts = [c for c in counts if c > 0]
            if valid_counts:
                return min(valid_counts)

    if len(w) <= 3:
        return 1

    vowels = "aeiouy"
    count = 0
    prev_is_vowel = False
    for ch in w:
        is_vowel = ch in vowels
        if is_vowel and not prev_is_vowel:
            count += 1
        prev_is_vowel = is_vowel

    if w.endswith("e") and not w.endswith(("le", "ye")) and count > 1:
        count -= 1

    return max(1, count)


def extract_vowel_combo_offsets(word):
    # Vowel pairs color harmony by adding semitone offsets.
    combo_map = {
        "ai": 5,
        "au": 7,
        "ea": 4,
        "ee": 7,
        "ei": 9,
        "ie": 10,
        "io": 12,
        "oa": 5,
        "oe": 8,
        "oi": 11,
        "oo": 12,
        "ou": 14,
        "ue": 9,
        "ui": 6,
    }
    w = re.sub(r"[^a-z]", "", normalized_word_ascii(word))
    offsets = []
    for i in range(len(w) - 1):
        pair = w[i : i + 2]
        if pair in combo_map:
            offsets.append(combo_map[pair])
    return offsets


VOWEL_PHONE_RE = re.compile(r"[AEIOU].*\d$")


def phonetic_syllable_units(word):
    key = word_key(word)
    if key in SPECIAL_UNIT_OVERRIDES:
        return SPECIAL_UNIT_OVERRIDES[key][:]

    w = re.sub(r"[^a-z']", "", normalized_word_ascii(word))
    if not w:
        return []
    if pronouncing is None:
        return []
    phones_list = pronouncing.phones_for_word(w)
    if not phones_list:
        return []

    phones = phones_list[0].split()
    units = []
    current = []
    for phone in phones:
        base = re.sub(r"\d", "", phone)
        current.append(base)
        if VOWEL_PHONE_RE.match(phone):
            units.append(".".join(current))
            current = []
    if current:
        if units:
            units[-1] = units[-1] + "." + ".".join(current)
        else:
            units.append(".".join(current))
    return units


def heuristic_syllable_units(word):
    key = word_key(word)
    if key in SPECIAL_UNIT_OVERRIDES:
        return SPECIAL_UNIT_OVERRIDES[key][:]

    w = re.sub(r"[^a-z]", "", normalized_word_ascii(word))
    if not w:
        return []
    units = []
    start = 0
    vowels = "aeiouy"
    i = 1
    while i < len(w):
        prev_is_vowel = w[i - 1] in vowels
        cur_is_vowel = w[i] in vowels
        # Cut near vowel-consonant boundaries to approximate spoken chunks.
        if prev_is_vowel and not cur_is_vowel:
            units.append(w[start:i])
            start = i
        i += 1
    units.append(w[start:])
    return [u for u in units if u]


def syllable_units(word):
    units = phonetic_syllable_units(word)
    if units:
        return units
    return heuristic_syllable_units(word)


def is_name_like(word):
    return len(word) > 1 and word[0].isupper() and any(ch.islower() for ch in word[1:])


def parse_key_root(key_input):
    key = key_input.strip().upper()
    return NOTE_NAME_TO_PC.get(key)


def parse_mode(mode_input):
    mode = mode_input.strip().lower()
    aliases = {
        "ionian": "major",
        "aeolian": "minor",
        "maj": "major",
        "min": "minor",
        "major pentatonic": "major_pentatonic",
        "minor pentatonic": "minor_pentatonic",
        "maj pent": "major_pentatonic",
        "min pent": "minor_pentatonic",
        "pentatonic major": "major_pentatonic",
        "pentatonic minor": "minor_pentatonic",
        "maj7": "major7",
        "major 7": "major7",
        "major7th": "major7",
        "major 7th": "major7",
        "chroma": "chromatic",
        "chromatic scale": "chromatic",
        "min7": "minor7",
        "minor 7": "minor7",
        "minor7th": "minor7",
        "minor 7th": "minor7",
    }
    normalized = aliases.get(mode, mode)
    return normalized if normalized in MODE_INTERVALS else None


def quantize_note_to_scale(note, key_root_pc, mode_intervals, low=DEFAULT_LOW_NOTE, high=DEFAULT_HIGH_NOTE):
    in_range = max(low, min(high, note))
    candidates = []
    for octave in range(-2, 11):
        base = 12 * octave
        for interval in mode_intervals:
            n = base + key_root_pc + interval
            if low <= n <= high:
                candidates.append(n)
    if not candidates:
        return in_range
    return min(candidates, key=lambda n: (abs(n - in_range), n))


def scale_notes_in_range(key_root_pc, mode_intervals, low=DEFAULT_LOW_NOTE, high=DEFAULT_HIGH_NOTE):
    notes = []
    for n in range(low, high + 1):
        if (n - key_root_pc) % 12 in mode_intervals:
            notes.append(n)
    return notes


def shift_note_by_scale_degree(note, degree_shift, key_root_pc, mode_intervals, low=DEFAULT_LOW_NOTE, high=DEFAULT_HIGH_NOTE):
    if degree_shift == 0:
        return max(low, min(high, note))
    scale_notes = scale_notes_in_range(key_root_pc, mode_intervals, low=low, high=high)
    if not scale_notes:
        return max(low, min(high, note))
    nearest_idx = min(range(len(scale_notes)), key=lambda i: (abs(scale_notes[i] - note), scale_notes[i]))
    target_idx = max(0, min(len(scale_notes) - 1, nearest_idx + degree_shift))
    return scale_notes[target_idx]


def transform_events_pitch(
    events,
    key_root_pc,
    mode_intervals,
    octave_shift=0,
    degree_shift=0,
    low=DEFAULT_LOW_NOTE,
    high=DEFAULT_HIGH_NOTE,
):
    out = []
    octave_shift = int(octave_shift)
    degree_shift = int(degree_shift)
    semitone_shift = octave_shift * 12
    for event in events:
        e = dict(event)
        chord = list(e.get("chord", []))
        if chord:
            shifted = []
            for note in chord:
                n = note + semitone_shift
                n = shift_note_by_scale_degree(n, degree_shift, key_root_pc, mode_intervals, low=low, high=high)
                n = quantize_note_to_scale(n, key_root_pc, mode_intervals, low=low, high=high)
                shifted.append(n)
            e["chord"] = shifted
        out.append(e)
    return out


def _normalize_voicing_mode(voicing_mode):
    mode = str(voicing_mode or "closed").strip().lower()
    return "open" if mode == "open" else "closed"


def apply_voicing_to_chord(chord, voicing_mode="closed", low=DEFAULT_LOW_NOTE, high=DEFAULT_HIGH_NOTE):
    notes = sorted(set(int(n) for n in chord))
    if len(notes) <= 1:
        return notes

    mode = _normalize_voicing_mode(voicing_mode)
    if mode == "closed":
        root = notes[0]
        closed = []
        for n in notes:
            c = n
            while c - root > 11 and c - 12 >= low:
                c -= 12
            closed.append(c)
        return sorted(set(max(low, min(high, n)) for n in closed))

    opened = [notes[0]]
    for n in notes[1:]:
        c = n
        while c - opened[-1] < 5 and c + 12 <= high:
            c += 12
        opened.append(c)
    return sorted(set(max(low, min(high, n)) for n in opened))


def apply_voicing_to_events(events, voicing_mode="closed", low=DEFAULT_LOW_NOTE, high=DEFAULT_HIGH_NOTE):
    mode = _normalize_voicing_mode(voicing_mode)
    out = []
    for event in events:
        e = dict(event)
        chord = list(e.get("chord", []))
        if chord:
            e["chord"] = apply_voicing_to_chord(chord, voicing_mode=mode, low=low, high=high)
        out.append(e)
    return out


def word_to_chord(word, low=DEFAULT_LOW_NOTE, high=DEFAULT_HIGH_NOTE, key_root_pc=0, mode_intervals=None):
    if mode_intervals is None:
        mode_intervals = MODE_INTERVALS["major"]
    units = syllable_units(word)
    syllables = count_syllables(word)
    num_notes = max(1, min(6, len(units) if units else syllables))
    intervals = [0, 4, 7, 10, 14, 17]
    combo_offsets = extract_vowel_combo_offsets(word)

    max_interval = intervals[num_notes - 1] if num_notes > 0 else 0
    max_combo = max(combo_offsets) if combo_offsets else 0
    octave_spread = 12 if combo_offsets and num_notes > 1 else 0
    root_high = max(low, high - (max_interval + max_combo + octave_spread))
    span = root_high - low + 1
    if units:
        # Prefix-anchored root: words sharing initial syllables map similarly.
        prefix_sig = "|".join(units[:2])
        tail_sig = "|".join(units[2:]) if len(units) > 2 else ""
        root_seed = (sum(ord(c) for c in prefix_sig) * 3) + (sum(ord(c) for c in tail_sig) % 17)
        root = low + (root_seed % span)
    else:
        root = low + (sum(ord(c) for c in word) % span)

    if any(ch.isupper() for ch in word):
        root = max(low, root - 3)

    chord = []
    for i in range(num_notes):
        note = root + intervals[i]
        if units:
            unit = units[i % len(units)]
            # Small phonetic color from the individual syllable.
            note += (sum(ord(c) for c in unit) % 4) - 1
        if combo_offsets:
            note += combo_offsets[i % len(combo_offsets)]
            if i % 2 == 1:
                note += 12
        quantized = quantize_note_to_scale(note, key_root_pc, mode_intervals, low=low, high=high)
        chord.append(quantized)
    return sorted(set(chord))


def widen_low_register_intervals(chord, low=DEFAULT_LOW_NOTE, high=DEFAULT_HIGH_NOTE):
    """
    Avoid cramped low-register intervals by widening adjacent notes to at least
    a fifth when possible.
    """
    if len(chord) < 2:
        return chord

    notes = sorted(chord)
    widened = [notes[0]]
    for note in notes[1:]:
        candidate = note
        prev = widened[-1]
        if prev <= LOW_INTERVAL_PIVOT:
            while candidate - prev < MIN_LOW_INTERVAL and candidate + 12 <= high:
                candidate += 12
        widened.append(candidate)

    # Keep range-safe and unique while preserving order as much as possible.
    safe = []
    for n in widened:
        n = max(low, min(high, n))
        if n not in safe:
            safe.append(n)
    return safe


def enrich_single_word_chord(
    chord,
    word,
    syllables,
    key_root_pc,
    mode_intervals,
    low=DEFAULT_LOW_NOTE,
    high=DEFAULT_HIGH_NOTE,
):
    """
    Single-word inputs can feel too sparse; add gentle support tones so the word
    stands on its own while retaining recognizability.
    """
    if not chord:
        return chord

    enriched = list(chord)
    root = min(chord)
    top = max(chord)

    is_short_word = len(re.sub(r"[^A-Za-z0-9]", "", word)) <= 4 and syllables <= 1
    if is_short_word:
        # Keep very short standalone words subtle: core note + two supports.
        support_targets = [root + 7, root + 12]
        max_notes = 3
    else:
        support_targets = [root + 7, root + 12, top + 5]
        max_notes = 4

    for target in support_targets:
        quantized = quantize_note_to_scale(target, key_root_pc, mode_intervals, low=low, high=high)
        if quantized not in enriched:
            enriched.append(quantized)
        if len(enriched) >= max_notes:
            break

    return sorted(set(enriched))


def middle_out_order(notes):
    if not notes:
        return []
    n = len(notes)
    left = (n - 1) // 2
    right = left + 1
    order = [notes[left]]
    while left - 1 >= 0 or right < n:
        if right < n:
            order.append(notes[right])
            right += 1
        if left - 1 >= 0:
            left -= 1
            order.append(notes[left])
    return order


def choose_chord_articulation(chord, stressed, token="", syllables=1):
    if len(chord) <= 1:
        return chord, []

    clean_len = len(re.sub(r"[^A-Za-z0-9]", "", str(token or "")))
    syl = max(1, int(syllables or 1))
    longness = max(clean_len, syl * 2)

    # Longer/multi-syllable words should roll/arpeggiate more often.
    # Short words remain tighter/more block-like.
    long_factor = max(0.0, min(1.0, (longness - 4) / 8.0))
    block_threshold = 0.42 - (0.30 * long_factor)  # ~0.42 -> ~0.12
    light_threshold = 0.74 - (0.16 * long_factor)  # ~0.74 -> ~0.58

    shape_roll = random.random()
    if shape_roll < block_threshold:
        shape = "block"
    elif shape_roll < light_threshold:
        shape = "light"
    else:
        shape = "arp"

    direction_roll = random.random()
    if direction_roll < 0.35:
        ordered = chord[:]  # up
    elif direction_roll < 0.60:
        ordered = list(reversed(chord))  # down
    elif direction_roll < 0.85:
        ordered = middle_out_order(chord)  # middle-out
    else:
        ordered = random.sample(chord, len(chord))  # random

    if shape == "block":
        steps = [0.0] * (len(ordered) - 1)
    else:
        base = ARP_STEP_BASE_SEC * (0.85 if stressed else 1.0)
        # Longer words get slightly wider step times -> more audible rolling.
        base *= (1.0 + 0.45 * long_factor)
        spread = (0.008 if shape == "light" else 0.014) * (1.0 + 0.40 * long_factor)
        steps = []
        for _ in range(len(ordered) - 1):
            step = base + random.uniform(-spread, spread)
            min_step = 0.0 if shape == "light" else 0.006
            steps.append(max(min_step, step))

    return ordered, steps


def is_stressed_word(word):
    w = re.sub(r"[^a-z]", "", normalized_word_ascii(word))
    if not w:
        return False
    if pronouncing is not None:
        phones = pronouncing.phones_for_word(w)
        if phones:
            stress = pronouncing.stresses(phones[0])
            return "1" in stress or "2" in stress
    return count_syllables(w) == 1


def choose_velocity(stressed, syllables, prev_punct="", word_index=0):
    # Speech-like dynamics: stressed words and phrase starts get slight accents.
    base = 78 + min(10, syllables * 2)
    if stressed:
        base += 9
    if prev_punct in {".", "!", "?"}:
        base += 8
    elif prev_punct in {",", ";", ":"}:
        base += 4
    if word_index == 0:
        base += 4

    jitter = random.randint(-7, 7)
    return max(45, min(122, base + jitter))


def choose_word_gap(word, next_token, stressed, syllables):
    # Keep word gaps tight/connected; reserve bigger gaps for punctuation.
    connectors = {"a", "an", "and", "as", "at", "by", "for", "in", "of", "on", "or", "the", "to", "with"}
    if next_token in {".", ",", "!", "?", ";", ":"}:
        return 0.0

    gap = random.uniform(-0.02, 0.01)
    if stressed and syllables >= 3:
        gap += 0.007
    if word.lower() in connectors:
        gap -= 0.012

    return max(-0.045, min(0.02, gap))


def count_words_to_sentence_end(tokens, start_idx):
    count = 0
    for token in tokens[start_idx:]:
        if token in {".", "!", "?"}:
            break
        if WORD_ONLY_RE.fullmatch(token):
            count += 1
    return max(1, count)


def phrase_rubato_multiplier(position_in_phrase):
    # Slower at phrase edges, quicker in the middle (speech-like rubato).
    edge = abs(position_in_phrase - 0.5) * 2.0  # 1 at edges, 0 in the middle
    base = 0.88 + (0.22 * edge)  # ~1.10 edges, ~0.88 middle
    human = random.uniform(-0.015, 0.015)
    return max(0.80, min(1.18, base + human))


def text_to_events(text, low=DEFAULT_LOW_NOTE, high=DEFAULT_HIGH_NOTE, key_root_pc=0, mode_intervals=None):
    if mode_intervals is None:
        mode_intervals = MODE_INTERVALS["major"]
    tokens = WORD_TOKEN_RE.findall(text)
    events = []

    punctuation_pauses = {
        ",": 0.14,
        ";": 0.20,
        ":": 0.20,
        ".": 0.32,
        "!": 0.32,
        "?": 0.32,
    }

    prev_punct = ""
    word_index = 0
    phrase_word_index = 0
    phrase_word_total = 1
    word_tokens = [t for t in tokens if WORD_ONLY_RE.fullmatch(t)]
    single_word_mode = len(word_tokens) == 1

    for i, token in enumerate(tokens):
        next_token = tokens[i + 1] if i + 1 < len(tokens) else ""
        if token in punctuation_pauses:
            pause = punctuation_pauses[token]
            events.append({"chord": [], "duration_sec": pause, "advance_sec": pause, "velocity": 0})
            prev_punct = token
            if token in {".", "!", "?"}:
                phrase_word_index = 0
                phrase_word_total = 1
            continue

        if phrase_word_index == 0:
            phrase_word_total = count_words_to_sentence_end(tokens, i)

        syllables = count_syllables(token)
        chord = word_to_chord(
            token,
            low=low,
            high=high,
            key_root_pc=key_root_pc,
            mode_intervals=mode_intervals,
        )
        stressed = is_stressed_word(token)
        has_upper = any(ch.isupper() for ch in token)
        name_like = is_name_like(token)

        if chord and has_upper:
            sentence_start = word_index == 0 or prev_punct in {".", "!", "?"}
            anchor_shift = 9
            if name_like:
                anchor_shift += 3
            if sentence_start:
                anchor_shift += 5
            anchor_note = quantize_note_to_scale(
                min(chord) - anchor_shift,
                key_root_pc,
                mode_intervals,
                low=low,
                high=high,
            )
            chord = sorted(set(chord + [anchor_note]))
            if name_like and sentence_start:
                second_anchor = quantize_note_to_scale(
                    anchor_note - 7,
                    key_root_pc,
                    mode_intervals,
                    low=low,
                    high=high,
                )
                chord = sorted(set(chord + [second_anchor]))

        chord = widen_low_register_intervals(chord, low=low, high=high)
        if single_word_mode:
            chord = enrich_single_word_chord(
                chord,
                token,
                syllables,
                key_root_pc,
                mode_intervals,
                low=low,
                high=high,
            )

        base_duration = 0.10 + (0.04 * min(syllables, 5))
        duration = base_duration * (1.20 if stressed else 0.92)
        duration += random.uniform(-0.015, 0.015)
        duration = max(0.06, duration)
        if single_word_mode:
            duration *= 1.20
        gap = choose_word_gap(token, next_token, stressed, syllables)
        advance = max(0.02, duration + gap)

        if phrase_word_total > 1:
            phrase_pos = phrase_word_index / (phrase_word_total - 1)
        else:
            phrase_pos = 0.0
        rubato = phrase_rubato_multiplier(phrase_pos)
        advance = max(0.02, advance * rubato)
        duration = max(0.06, duration * (0.96 + 0.04 * rubato))

        velocity = choose_velocity(stressed, syllables, prev_punct=prev_punct, word_index=word_index)
        if name_like:
            velocity = min(122, velocity + 4)
        if single_word_mode:
            velocity = min(122, velocity + 5)

        ordered_chord, arpeggio_steps_sec = choose_chord_articulation(
            chord,
            stressed,
            token=token,
            syllables=syllables,
        )

        events.append(
            {
                "chord": ordered_chord,
                "duration_sec": duration,
                "advance_sec": advance,
                "velocity": velocity,
                "arpeggio_steps_sec": arpeggio_steps_sec,
                "source_token": token,
                "source_units": syllable_units(token),
                "source_syllables": syllables,
            }
        )
        prev_punct = ""
        word_index += 1
        phrase_word_index += 1

    return events


def add_pitch_bend_to_events(events, bend_amount=0):
    """
    Add subtle pitch-bend envelopes per note event.
    bend_amount: 0..100 (0 = disabled).
    """
    amount = max(0, min(100, int(bend_amount)))
    if amount == 0:
        out = []
        for event in events:
            e = dict(event)
            e["bend_curve"] = []
            out.append(e)
        return out

    max_units = int(300 + (amount / 100.0) * 1300)
    out = []
    for event in events:
        e = dict(event)
        if not e.get("chord"):
            e["bend_curve"] = []
            out.append(e)
            continue

        velocity = int(e.get("velocity", 90))
        stressed_boost = 1.15 if velocity >= 98 else 1.0
        amp = int(max_units * random.uniform(0.55, 1.0) * stressed_boost)
        amp = max(0, min(MAX_PITCH_BEND, amp))
        direction = random.choice([-1, 1])
        start = int(direction * amp * 0.60)
        mid = int((-direction * amp * 0.28) + random.randint(-amp // 8, amp // 8))
        start = max(-MAX_PITCH_BEND, min(MAX_PITCH_BEND, start))
        mid = max(-MAX_PITCH_BEND, min(MAX_PITCH_BEND, mid))

        e["bend_curve"] = [
            (0.08, start),
            (0.42, mid),
            (0.82, 0),
        ]
        out.append(e)
    return out


def apply_tempo_to_events(events, tempo_bpm):
    tempo_scale = REFERENCE_TEMPO_BPM / float(tempo_bpm)
    scaled = []
    for event in events:
        scaled.append(
            {
                "chord": event["chord"],
                "duration_sec": event["duration_sec"] * tempo_scale,
                "advance_sec": event.get("advance_sec", event["duration_sec"]) * tempo_scale,
                "velocity": event["velocity"],
                "arpeggio_steps_sec": [step * tempo_scale for step in event.get("arpeggio_steps_sec", [])],
                "bend_curve": list(event.get("bend_curve", [])),
                "source_token": event.get("source_token", ""),
                "source_units": list(event.get("source_units", []) or []),
                "source_syllables": int(event.get("source_syllables", 0) or 0),
            }
        )
    return scaled


def flush_due_notes(outport, active_notes):
    now = time.monotonic()
    remaining = []
    for off_time, note in active_notes:
        if off_time <= now:
            outport.send(Message("note_off", note=note, velocity=0))
        else:
            remaining.append((off_time, note))
    active_notes[:] = remaining


def sleep_with_note_flush(outport, active_notes, seconds, stop_requested=None):
    end_time = time.monotonic() + max(0.0, seconds)
    while True:
        if stop_requested and stop_requested():
            return False
        flush_due_notes(outport, active_notes)
        now = time.monotonic()
        if now >= end_time:
            break
        step = min(0.01, end_time - now)
        if active_notes:
            next_off = min(t for t, _ in active_notes)
            step = min(step, max(0.0, next_off - now))
        if step <= 0:
            continue
        time.sleep(step)
    flush_due_notes(outport, active_notes)
    return True


def emit_note_callback(
    note_callback,
    note,
    velocity,
    source_token,
    source_units,
    source_syllables,
    note_index,
    chord_size,
):
    if not note_callback:
        return
    try:
        note_callback(
            note,
            velocity,
            source_token,
            source_units,
            source_syllables,
            note_index=note_index,
            chord_size=chord_size,
        )
        return
    except TypeError:
        # Backward-compatible callback signature.
        pass
    try:
        note_callback(
            note,
            velocity,
            source_token,
            source_units,
            source_syllables,
        )
    except Exception:
        pass


def send_live(port_name, events, stop_requested=None, note_callback=None):
    require_mido()
    with mido.open_output(port_name) as outport:
        active_notes = []
        stopped = False
        for event in events:
            if stop_requested and stop_requested():
                stopped = True
                break
            chord = event["chord"]
            duration = event["duration_sec"]
            advance = event.get("advance_sec", duration)
            velocity = event["velocity"]
            arpeggio_steps_sec = event.get("arpeggio_steps_sec", [])
            bend_curve = event.get("bend_curve", [])
            if not chord:
                if not sleep_with_note_flush(outport, active_notes, advance, stop_requested=stop_requested):
                    stopped = True
                    break
                continue

            event_interrupted = False
            for i, note in enumerate(chord):
                outport.send(Message('note_on', note=note, velocity=velocity))
                emit_note_callback(
                    note_callback,
                    note,
                    velocity,
                    event.get("source_token", ""),
                    event.get("source_units", []),
                    event.get("source_syllables", 0),
                    note_index=i,
                    chord_size=len(chord),
                )
                active_notes.append((time.monotonic() + max(0.03, duration), note))
                if i < len(chord) - 1:
                    step = arpeggio_steps_sec[i] if i < len(arpeggio_steps_sec) else ARP_STEP_BASE_SEC
                    if not sleep_with_note_flush(outport, active_notes, step, stop_requested=stop_requested):
                        event_interrupted = True
                        stopped = True
                        break
            if event_interrupted:
                break

            spread_sec = sum(arpeggio_steps_sec)
            remaining_advance = max(0.0, advance - spread_sec)
            if bend_curve and remaining_advance > 0:
                bend_window = min(max(0.03, duration - spread_sec), remaining_advance)
                elapsed = 0.0
                for frac, bend_val in bend_curve:
                    target = min(remaining_advance, max(0.0, frac * bend_window))
                    if target > elapsed:
                        if not sleep_with_note_flush(
                            outport, active_notes, target - elapsed, stop_requested=stop_requested
                        ):
                            event_interrupted = True
                            stopped = True
                            break
                    outport.send(Message("pitchwheel", pitch=int(bend_val)))
                    elapsed = target
                if event_interrupted:
                    break
                if remaining_advance > elapsed:
                    if not sleep_with_note_flush(
                        outport, active_notes, remaining_advance - elapsed, stop_requested=stop_requested
                    ):
                        stopped = True
                        break
                outport.send(Message("pitchwheel", pitch=0))
            else:
                if not sleep_with_note_flush(outport, active_notes, remaining_advance, stop_requested=stop_requested):
                    stopped = True
                    break

        if active_notes:
            tail = max(0.0, max(t for t, _ in active_notes) - time.monotonic())
            sleep_with_note_flush(outport, active_notes, tail, stop_requested=stop_requested)
            for _, note in active_notes:
                outport.send(Message("note_off", note=note, velocity=0))
        outport.send(Message("pitchwheel", pitch=0))
        return not stopped


def send_live_reactive(port_name, base_events, state_provider, stop_requested=None, note_callback=None):
    """
    Send events while re-reading playback settings before each event.
    This allows tempo/key/mode/shift/bend changes to affect an ongoing
    non-loop send without restarting.
    """
    require_mido()
    with mido.open_output(port_name) as outport:
        active_notes = []
        stopped = False
        for raw_event in base_events:
            if stop_requested and stop_requested():
                stopped = True
                break

            state = state_provider() or {}
            key_root_pc = state.get("key_root_pc", NOTE_NAME_TO_PC["C"])
            mode_intervals = state.get("mode_intervals", MODE_INTERVALS["major"])
            octave_shift = int(state.get("octave_shift", 0))
            degree_shift = int(state.get("degree_shift", 0))
            bend_amount = int(state.get("bend_amount", 0))
            tempo_bpm = max(20, min(300, int(state.get("tempo_bpm", 120))))
            voicing_mode = state.get("voicing_mode", "closed")

            event = transform_events_pitch(
                [raw_event],
                key_root_pc=key_root_pc,
                mode_intervals=mode_intervals,
                octave_shift=octave_shift,
                degree_shift=degree_shift,
            )[0]
            event = apply_voicing_to_events([event], voicing_mode=voicing_mode)[0]
            event = add_pitch_bend_to_events([event], bend_amount=bend_amount)[0]
            event = apply_tempo_to_events([event], tempo_bpm)[0]

            chord = event["chord"]
            duration = event["duration_sec"]
            advance = event.get("advance_sec", duration)
            velocity = event["velocity"]
            arpeggio_steps_sec = event.get("arpeggio_steps_sec", [])
            bend_curve = event.get("bend_curve", [])

            if not chord:
                if not sleep_with_note_flush(outport, active_notes, advance, stop_requested=stop_requested):
                    stopped = True
                    break
                continue

            event_interrupted = False
            for i, note in enumerate(chord):
                outport.send(Message("note_on", note=note, velocity=velocity))
                emit_note_callback(
                    note_callback,
                    note,
                    velocity,
                    event.get("source_token", ""),
                    event.get("source_units", []),
                    event.get("source_syllables", 0),
                    note_index=i,
                    chord_size=len(chord),
                )
                active_notes.append((time.monotonic() + max(0.03, duration), note))
                if i < len(chord) - 1:
                    step = arpeggio_steps_sec[i] if i < len(arpeggio_steps_sec) else ARP_STEP_BASE_SEC
                    if not sleep_with_note_flush(outport, active_notes, step, stop_requested=stop_requested):
                        event_interrupted = True
                        stopped = True
                        break
            if event_interrupted:
                break

            spread_sec = sum(arpeggio_steps_sec)
            remaining_advance = max(0.0, advance - spread_sec)
            if bend_curve and remaining_advance > 0:
                bend_window = min(max(0.03, duration - spread_sec), remaining_advance)
                elapsed = 0.0
                for frac, bend_val in bend_curve:
                    target = min(remaining_advance, max(0.0, frac * bend_window))
                    if target > elapsed:
                        if not sleep_with_note_flush(
                            outport, active_notes, target - elapsed, stop_requested=stop_requested
                        ):
                            event_interrupted = True
                            stopped = True
                            break
                    outport.send(Message("pitchwheel", pitch=int(bend_val)))
                    elapsed = target
                if event_interrupted:
                    break
                if remaining_advance > elapsed:
                    if not sleep_with_note_flush(
                        outport, active_notes, remaining_advance - elapsed, stop_requested=stop_requested
                    ):
                        stopped = True
                        break
                outport.send(Message("pitchwheel", pitch=0))
            else:
                if not sleep_with_note_flush(outport, active_notes, remaining_advance, stop_requested=stop_requested):
                    stopped = True
                    break

        if active_notes:
            tail = max(0.0, max(t for t, _ in active_notes) - time.monotonic())
            sleep_with_note_flush(outport, active_notes, tail, stop_requested=stop_requested)
            for _, note in active_notes:
                outport.send(Message("note_off", note=note, velocity=0))
        outport.send(Message("pitchwheel", pitch=0))
        return not stopped


def save_midi(filename, events, source_text="", ticks_per_beat=480, tempo_bpm=120):
    require_mido()
    midi = MidiFile(ticks_per_beat=ticks_per_beat)
    track = MidiTrack()
    midi.tracks.append(track)

    if source_text:
        track.append(MetaMessage("track_name", name="TextToMIDI", time=0))
        track.append(MetaMessage("text", text=source_text, time=0))

    tempo = mido.bpm2tempo(tempo_bpm)
    track.append(MetaMessage('set_tempo', tempo=tempo, time=0))
    # Convert the event timing using a fixed reference grid, then let set_tempo
    # control real playback speed in the DAW.
    ticks_per_second = (ticks_per_beat * REFERENCE_TEMPO_BPM) / 60.0
    current_tick = 0
    scheduled = []

    for event in events:
        chord = event["chord"]
        duration_ticks = max(1, int(round(event["duration_sec"] * ticks_per_second)))
        advance_ticks = max(0, int(round(event.get("advance_sec", event["duration_sec"]) * ticks_per_second)))
        velocity = event["velocity"]
        arpeggio_steps_sec = event.get("arpeggio_steps_sec", [])
        arpeggio_steps_ticks = [max(0, int(round(step * ticks_per_second))) for step in arpeggio_steps_sec]
        bend_curve = event.get("bend_curve", [])

        if chord:
            step_sum = 0
            on_ticks = []
            for i, note in enumerate(chord):
                if i > 0:
                    step_idx = i - 1
                    step_sum += arpeggio_steps_ticks[step_idx] if step_idx < len(arpeggio_steps_ticks) else 0
                on_tick = current_tick + step_sum
                off_tick = on_tick + duration_ticks
                on_ticks.append(on_tick)
                scheduled.append((on_tick, 1, Message("note_on", note=note, velocity=velocity, time=0)))
                scheduled.append((off_tick, 0, Message("note_off", note=note, velocity=0, time=0)))

            if bend_curve and on_ticks:
                event_start = min(on_ticks)
                event_end = max(on_tick + duration_ticks for on_tick in on_ticks)
                bend_window = max(1, event_end - event_start)
                for frac, bend_val in bend_curve:
                    bend_tick = event_start + int(round(max(0.0, min(1.0, frac)) * bend_window))
                    bend_tick = max(event_start, min(event_end, bend_tick))
                    bend_pitch = int(max(-MAX_PITCH_BEND, min(MAX_PITCH_BEND, bend_val)))
                    scheduled.append((bend_tick, 2, Message("pitchwheel", pitch=bend_pitch, time=0)))
                scheduled.append((event_end, 3, Message("pitchwheel", pitch=0, time=0)))

        current_tick += advance_ticks

    scheduled.sort(key=lambda item: (item[0], item[1]))
    last_tick = 0
    for tick, _, msg in scheduled:
        msg.time = max(0, tick - last_tick)
        track.append(msg)
        last_tick = tick

    midi.save(filename)


def midi_note_to_name(note):
    names = ["C", "C#", "D", "D#", "E", "F", "F#", "G", "G#", "A", "A#", "B"]
    octave = (note // 12) - 1
    return f"{names[note % 12]}{octave}"


def build_output_filename(text):
    base = text.strip()[:32]
    if not base:
        base = "output"
    base = re.sub(r"\s+", "_", base)
    base = re.sub(r"[^A-Za-z0-9_-]", "", base)
    if not base:
        base = "output"
    return f"{base}.mid"


def build_output_path(text):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUTPUT_DIR / build_output_filename(text)


def key_name_from_pc(pc):
    return PC_TO_NOTE_NAME.get(pc % 12, "C")


def prompt_session_settings(default_tempo=120, default_key="C", default_mode="major"):
    tempo_input = input(f"Tempo BPM for this session (press Enter for {default_tempo}): ").strip()
    if tempo_input:
        try:
            tempo_bpm = max(20, min(300, int(tempo_input)))
        except ValueError:
            tempo_bpm = default_tempo
    else:
        tempo_bpm = default_tempo

    key_input = input(f"Key for this session (e.g. C, F#, Bb; press Enter for {default_key}): ").strip()
    parsed_key = parse_key_root(key_input) if key_input else parse_key_root(default_key)
    if parsed_key is None:
        parsed_key = parse_key_root(default_key)

    mode_input = input(
        "Mode for this session (major/minor/dorian/phrygian/lydian/mixolydian/locrian; "
        f"Enter for {default_mode}): "
    ).strip()
    parsed_mode = parse_mode(mode_input) if mode_input else default_mode
    if parsed_mode is None:
        parsed_mode = default_mode

    return tempo_bpm, parsed_key, parsed_mode


if __name__ == "__main__":
    ports = mido.get_output_names()

    if not ports:
        raise RuntimeError("No MIDI output ports found.")

    selected_port = ports[0]
    session_tempo_bpm, session_key_root_pc, session_mode_name = prompt_session_settings(
        default_tempo=120,
        default_key="C",
        default_mode="major",
    )
    session_mode_intervals = MODE_INTERVALS[session_mode_name]
    print("Commands: /settings /tempo <bpm> /key <note> /mode <mode> /status /help /quit")

    while True:
        text = input("Type text to convert to MIDI (or 'quit'): ").strip()
        lowered = text.lower()
        if lowered in {"quit", "q", "exit", "/quit"}:
            break
        if not text:
            continue

        if lowered in {"/help", "help"}:
            print("Commands: /settings /tempo <bpm> /key <note> /mode <mode> /status /help /quit")
            continue
        if lowered in {"/status", "status"}:
            print(f"Session: tempo={session_tempo_bpm} key={key_name_from_pc(session_key_root_pc)} mode={session_mode_name}")
            continue
        if lowered in {"/settings", "settings", "/config", "config"}:
            session_tempo_bpm, session_key_root_pc, session_mode_name = prompt_session_settings(
                default_tempo=session_tempo_bpm,
                default_key=key_name_from_pc(session_key_root_pc),
                default_mode=session_mode_name,
            )
            session_mode_intervals = MODE_INTERVALS[session_mode_name]
            print(
                f"Updated: tempo={session_tempo_bpm} key={key_name_from_pc(session_key_root_pc)} mode={session_mode_name}"
            )
            continue
        if lowered.startswith("/tempo "):
            value = text.split(None, 1)[1].strip()
            try:
                session_tempo_bpm = max(20, min(300, int(value)))
                print(f"Updated tempo: {session_tempo_bpm}")
            except ValueError:
                print("Invalid tempo. Use: /tempo 120")
            continue
        if lowered.startswith("/key "):
            value = text.split(None, 1)[1].strip()
            parsed_key = parse_key_root(value)
            if parsed_key is None:
                print("Invalid key. Use examples: C, F#, Bb")
            else:
                session_key_root_pc = parsed_key
                print(f"Updated key: {key_name_from_pc(session_key_root_pc)}")
            continue
        if lowered.startswith("/mode "):
            value = text.split(None, 1)[1].strip()
            parsed_mode = parse_mode(value)
            if parsed_mode is None:
                print("Invalid mode. Try: major minor dorian phrygian lydian mixolydian locrian")
            else:
                session_mode_name = parsed_mode
                session_mode_intervals = MODE_INTERVALS[session_mode_name]
                print(f"Updated mode: {session_mode_name}")
            continue

        events = text_to_events(
            text,
            key_root_pc=session_key_root_pc,
            mode_intervals=session_mode_intervals,
        )
        note_events = [event for event in events if event["chord"]]
        if not note_events:
            continue

        timed_events = apply_tempo_to_events(events, session_tempo_bpm)
        send_live(selected_port, timed_events)
        sent_note_names = [[midi_note_to_name(n) for n in event["chord"]] for event in note_events]
        print("MIDI sent")
        print(f"Notes sent: {sent_note_names}")

        output_path = build_output_path(text)
        save_midi(str(output_path), events, source_text=text, tempo_bpm=session_tempo_bpm)
