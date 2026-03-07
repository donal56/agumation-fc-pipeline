# Agumation Fan Club Pipeline
This is a tool to translate videos. It's structured like a pipeline where videos will be consumed in batch and will progress multiple stages. 

The output is a video with hardcoded subs in spanish and english, the source language is japanese.


## Pipeline
The tool is composed of the following stages. A stage is required to success to procced to the next.

- If you want a stage to re-run for certan file just delete the associated output.
- If you want to mark QC issues as solved, delete the file contents.
- The QC job does easy-readibility validations, please review the outputs from stage 4 and 5 to assure accurate translation.

| # | Stage                 | Output                                | Folder                |
|---|-----------------------|---------------------------------------|-----------------------|
| 1 | Transcribe            | SRT file in japanese                  | `/pipeline/1_srt-jp`  |
| 2 | Quality check         | Plain text file with detected errors  | `/pipeline/2_qc`      |
| 3 | Translate to english  | SRT file in english                   | `/pipeline/3_srt-en`  |
| 4 | Translate to spanish  | SRT file in spanish                   | `/pipeline/4_srt-sp`  |
| 5 | Hardsub               | Final video with hardcoded subs       | `/pipeline/5_output`  |

## Requeriments
 - Python 3.9+
 - FFmpeg

> Important: Both should be accesible via PATH

## Setup
Default setup for Windows (safe route, recommended):

> Note: Change "3.12" with the python version you have instlaled

1.- Create transcription environment:

    py -3.12 -m venv venv_transcribe
    .\venv_transcribe\Scripts\Activate.ps1
    python -m pip install --upgrade pip
    python -m pip install --only-binary :all: faster-whisper==1.2.1 ctranslate2==4.4.0 setuptools==80.9.0
    deactivate

2.- Create translation environment:

    py -3.12 -m venv venv_translate
    .\venv_translate\Scripts\Activate.ps1
    python -m pip install --upgrade pip
    python -m pip install --only-binary :all: ctranslate2==4.4.0 setuptools==80.9.0
    python -m pip install sentencepiece==0.2.1 sacremoses==0.1.1 packaging
    python -m pip install argostranslate==1.9.6 --no-deps
    python .\scripts\setup\install_argos_models.py
    deactivate

3.- Validate instalation:

    python .\scripts\setup\init_pipeline_dirs.py
    .\venv_transcribe\Scripts\python.exe .\scripts\setup\validate_transcribe_runtime.py
    .\venv_translate\Scripts\python.exe .\scripts\setup\validate_translate_runtime.py

## Usage
Place the videos in `/pipeline/0_src`.

Then you can execute an specific stage:

    python run_pipeline.py transcribe
    python run_pipeline.py qc
    python run_pipeline.py translate_en
    python run_pipeline.py translate_es
    python run_pipeline.py hardsub

Or run the complete pipeline:

    python run_pipeline.py all

Script wrappers for Windows|Unix are available in `/script/wrappers`.

The final videos will be available in `/pipeline/5_output`.

## Customization

To fork this pipeline to use different language models, download them in [https://www.argosopentech.com/argospm/index/](https://www.argosopentech.com/argospm/index/), make sure to use more spoken languages as a bridge to less spoken ones (e.g. JP -> EN -> SP)
