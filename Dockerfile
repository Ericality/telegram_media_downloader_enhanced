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

# Install rclone from official binary (apt version uses Go 1.19 which has
# TLS/HTTP2 compatibility issues with Microsoft OneDrive upload endpoints)
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && ARCH=$(uname -m) \
    && case "$ARCH" in \
         x86_64)  RCLONE_ARCH=amd64 ;; \
         aarch64) RCLONE_ARCH=arm64 ;; \
         *)       RCLONE_ARCH=amd64 ;; \
       esac \
    && curl -fsSL "https://downloads.rclone.org/rclone-current-linux-${RCLONE_ARCH}.deb" -o /tmp/rclone.deb \
    && dpkg -i /tmp/rclone.deb \
    && rm /tmp/rclone.deb \
    && apt-get remove -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

COPY setup.py media_downloader.py /app/
COPY module /app/module
COPY utils /app/utils

# Allow any user to write parser cache files (PLY generates these at runtime)
RUN chmod -R 777 /app/module

# Ensure /app is writable by any user (rclone needs ~/.cache/rclone/ for token refresh)
RUN mkdir -p /app/.cache/rclone && chmod -R 777 /app

CMD ["python", "media_downloader.py"]
