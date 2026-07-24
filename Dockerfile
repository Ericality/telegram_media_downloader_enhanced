FROM python:3.11.9-slim

WORKDIR /app

# Install Pyrogram from local zip (avoids network issues in cross-arch builds)
COPY pyrogram-patch.zip /app/
RUN pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org /app/pyrogram-patch.zip \
    && rm /app/pyrogram-patch.zip

# Install remaining deps
COPY requirements.txt /app/
RUN apt-get update && apt-get install -y --no-install-recommends gcc \
    && pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements.txt \
    && apt-get remove -y gcc && apt-get autoremove -y && rm -rf /var/lib/apt/lists/* requirements.txt

# Install rclone
RUN apt-get update && apt-get install -y --no-install-recommends rclone \
    && rm -rf /var/lib/apt/lists/*

COPY setup.py media_downloader.py /app/
COPY module /app/module
COPY utils /app/utils

# Allow any user to write parser cache files (PLY generates these at runtime)
RUN chmod -R 777 /app/module

CMD ["python", "media_downloader.py"]