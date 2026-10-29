# Synthetic live-probe media

These fixtures contain no session or personal data. Expected answers are held by
the probe, not included in its text prompts.

- `image.png`: 640 × 160 black text on white, rendered from `image.svg` using
  ImageMagick: `magick -background white image.svg image.png`.
- `tool-image.png`: the same layout from `tool-image.svg`, with `ORANGE 19`.
- `document.pdf`: a one-page PDF with Helvetica text `COBALT 41`; no embedded files,
  scripts, external resources or encryption.
- `tool-document.pdf`: the same PDF structure, with `INDIGO 62`.
- `user.wav`: synthetic speech saying "Silver twenty six".
- `tool.wav`: synthetic speech saying "Amber fifty eight".
- `user.mp3`: synthetic speech saying "Golden thirty two".
- `tool.mp3`: synthetic speech saying "Scarlet eighty four".

Speech was generated with macOS `say -v Samantha -o <scratch.aiff> <phrase>` and
converted with FFmpeg to mono 16 kHz PCM WAV (`-ac 1 -ar 16000 -c:a pcm_s16le`)
and MP3 (`-ac 1 -ar 16000 -c:a libmp3lame -b:a 32k`). Distinct values for every
origin and audio format prevent the follow-up check from passing by copying the
baseline answer. The ChatGPT candidate MIME types are `audio/wav` and `audio/mpeg`;
neither is a claim of backend support.
The current ChatGPT probe excludes audio after the backend's measured refusal;
these synthetic audio files are retained but are not loaded or sent by that route.
