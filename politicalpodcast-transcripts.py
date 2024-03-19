import os, wget, re
from pydub import AudioSegment
from pydub.utils import make_chunks
import assemblyai as aai
from multiprocessing import Pool
import pandas as pd
import glob

current_dir = os.getcwd()
SOURCE_DIR = os.path.join(current_dir, 'source')
CHUNKS_DIR = os.path.join(current_dir, 'chunks')
TRANSCRIPTS_DIR = os.path.join(current_dir, 'transcripts')

os.makedirs(SOURCE_DIR, exist_ok=True)
os.makedirs(CHUNKS_DIR, exist_ok=True)
os.makedirs(TRANSCRIPTS_DIR, exist_ok=True)

# Replace with your API key
aai.settings.api_key = 'xxxxxxxxxxxxxxxxxxxxx'
TRANSCRIBER = aai.Transcriber()


def download_podcast(url, save_path):
    wget.download(url, out=save_path)


def mp3_to_wav(mp3_path, wav_path):
    sound = AudioSegment.from_mp3(mp3_path)
    sound.export(wav_path, format="wav")
    os.remove(mp3_path)


def chunk_wav(wav_path, chunk_size_ms):
    sound = AudioSegment.from_wav(wav_path, 'wav')

    # Make chunks of 30 secs
    chunks = make_chunks(sound, chunk_size_ms)

    for i, chunk in enumerate(chunks):
        chunk_name = CHUNKS_DIR + '/tmp_{0}.wav'.format(i)
        chunk.export(chunk_name, format="wav")


def convert_to_text(chunk_file):
    """
    Converts an audio file to speech using Googles Speech to Text
    params: data - the audio files that need to be converted to text
    return: text of the audio file
    """
    file_path = os.path.join(CHUNKS_DIR, chunk_file)
    try:
        transcript = TRANSCRIBER.transcribe(file_path)
        return transcript.text
    except Exception as e:
        print(f"Transcription failed for {chunk_file}: {e}")
        return ''


def save_text_to_file(text, file_path):
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)  # Create directory if not exists
        with open(file_path, 'w') as f:
            f.write(text)
        return True
    except Exception as e:
        print(f"Error saving text to file: {e}")
        return False


def remove_chunks():
    # Code for removing temporary chunk files
    files = glob.glob(os.path.join(CHUNKS_DIR, '*.wav'))
    for f in files:
        try:
            os.remove(f)
        except Exception as e:
            print(f"Error: {f} : {e.strerror}")


def process_podcast(row):
    podcaster = re.sub(' ', '', row['podcaster'])
    title = row['title']
    title = re.sub(' ', '', title)
    title = re.sub('"', '', title)
    title = re.sub(r'[,\'.]', '', title)
    title = re.sub(r'[^\w\s-]', '', title)
    pod_link = row['pod_link']
    mp3_path = f'{SOURCE_DIR}/{podcaster}/{title}.mp3'
    wav_path = f'{SOURCE_DIR}/{podcaster}/{title}.wav'
    text_file_path = f'{TRANSCRIPTS_DIR}/{podcaster}/{title}_transcript.txt'

    if os.path.isfile(text_file_path):
        return f'{title}_transcript.txt'

    # Create directory if not exists
    source_pod_dir = os.path.join(SOURCE_DIR, podcaster)
    os.makedirs(source_pod_dir, exist_ok=True)

    # Download podcast
    download_podcast(pod_link, mp3_path)

    # Convert to WAV
    mp3_to_wav(mp3_path, wav_path)

    # Chunk WAV file
    chunk_wav(wav_path, chunk_size_ms=30000)  # chunk size in milliseconds

    transcripts = []
    files = sorted(os.listdir(CHUNKS_DIR))
    with Pool(8) as pool:
        transcripts = pool.map(convert_to_text, files)

    # Check for None values in transcripts
    if None in transcripts:
        print(f"Transcription failed for {title}")
        return None

    # Save transcripts to file
    all_text = '\n'.join(t for t in transcripts if t is not None)
    save_text_to_file(all_text, text_file_path)

    # Remove all chunk files
    remove_chunks()

    print(f"Transcript saved for '{title}'")

    return f'{title}_transcript.txt'


if __name__ == "__main__":
    if not os.path.isfile(os.path.join(current_dir, 'process_podcasts.csv')):
        csv_file = 'politicalpodcasts.csv'  # Change this to your CSV file path
        random_row_count = 5  # Number of random rows to read for each podcaster
        # Read CSV into DataFrame
        df = pd.read_csv(os.path.join(current_dir, csv_file))
        # Get unique podcasters
        podcasters = df['podcaster'].unique()
        # Create an empty list to store processed rows
        processed_rows = []

        # Get random rows for each podcaster and append to processed_rows list
        for podcaster in podcasters:
            pod_rows = df[df['podcaster'] == podcaster]
            selected_rows = pod_rows.sample(min(len(pod_rows), random_row_count))
            processed_rows.extend(selected_rows.to_dict(orient='records'))

        # Convert processed_rows list to DataFrame
        processed_df = pd.DataFrame(processed_rows)

        # Save DataFrame to CSV
        processed_df.to_csv(os.path.join(current_dir, 'process_podcasts.csv'), index=False)

    csv_file = 'process_podcasts.csv'
    podcasts_df = pd.read_csv(os.path.join(current_dir, csv_file))
