"""Cloud drive upload support

Supports Rclone and Aligo adapters for uploading downloaded media to cloud storage.
"""
import asyncio
import functools
import importlib
import inspect
import json
import os
import re
from asyncio import subprocess
from datetime import datetime
from subprocess import Popen
from typing import Callable
from zipfile import ZipFile
import logging
from utils import platform

logger = logging.getLogger(__name__)
# pylint: disable = R0902
class CloudDriveConfig:
    """Rclone Config"""

    def __init__(
        self,
        enable_upload_file: bool = False,
        before_upload_file_zip: bool = False,
        after_upload_file_delete: bool = True,
        rclone_path: str = os.path.join(
            os.path.abspath("."), "rclone", f"rclone{platform.get_exe_ext()}"
        ),
        remote_dir: str = "",
        upload_adapter: str = "rclone",
    ):
        self.enable_upload_file = enable_upload_file
        self.before_upload_file_zip = before_upload_file_zip
        self.after_upload_file_delete = after_upload_file_delete
        self.rclone_path = rclone_path
        self.remote_dir = remote_dir
        self.upload_adapter = upload_adapter
        self.dir_cache: dict = {}  # for remote mkdir
        self.total_upload_success_file_count = 0
        self.aligo = None

    def pre_run(self):
        """pre run init aligo"""
        if self.enable_upload_file and self.upload_adapter == "aligo":
            CloudDrive.init_upload_adapter(self)


def _rclone_env() -> dict:
    """Return environment dict with HOME and RCLONE_CONFIG for subprocess calls."""
    env = os.environ.copy()
    # Ensure HOME is set so rclone can write token cache to ~/.cache/rclone/
    if "HOME" not in env or env.get("HOME") == "/":
        env["HOME"] = "/app"
    return env


async def verify_rclone_remote(drive_config: CloudDriveConfig) -> tuple:
    """Check if the configured rclone remote is accessible (read + write).

    Runs a full round-trip: list root → upload tiny file → verify → delete.
    
    Returns:
        (success: bool, message: str)
    """
    try:
        # Extract the root of the remote path (e.g., "OneDriveEricalitySha:" from "OneDriveEricalitySha:telegram/downloads")
        root_remote = drive_config.remote_dir.split(":")[0] + ":"

        # Ensure remote subdirectory exists
        subdir_cmd = f'"{drive_config.rclone_path}" mkdir "{drive_config.remote_dir.rstrip("/")}/"'
        subdir_proc = await asyncio.create_subprocess_shell(
            subdir_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=_rclone_env()
        )
        await asyncio.wait_for(subdir_proc.communicate(), timeout=15)
        # mkdir may return non-zero if directory already exists — that's OK

        # Write test: upload a tiny file
        test_file = "/tmp/_rclone_verify_test.txt"
        remote_test_path = f"{drive_config.remote_dir.rstrip('/')}/_rclone_verify_test.txt"

        with open(test_file, "w") as f:
            f.write("rclone verify test")

        logger.info(f"验证 Rclone 远程存储写入: {remote_test_path}")
        upload_cmd = f'"{drive_config.rclone_path}" copyto "{test_file}" "{remote_test_path}"'
        upload_proc = await asyncio.create_subprocess_shell(
            upload_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=_rclone_env()
        )
        upload_stdout, upload_stderr = await asyncio.wait_for(upload_proc.communicate(), timeout=60)

        upload_stderr_text = upload_stderr.decode(errors="replace") if upload_stderr else ""
        if upload_proc.returncode != 0:
            logger.warning(f"copyto 退出码非零: {upload_proc.returncode}，stderr: {upload_stderr_text[:300]}，将尝试验证文件内容")

        # Read back uploaded content to verify (lsf can see 0-byte stubs on OneDrive)
        verify_cmd = f'"{drive_config.rclone_path}" cat "{remote_test_path}"'
        verify_proc = await asyncio.create_subprocess_shell(
            verify_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=_rclone_env()
        )
        verify_stdout, _ = await asyncio.wait_for(verify_proc.communicate(), timeout=15)
        # Re-read local test file content for comparison
        with open(test_file, "r") as f:
            expected = f.read()
        uploaded_content = verify_stdout.decode(errors="replace") if verify_stdout else ""

        if uploaded_content != expected:
            if os.path.exists(test_file):
                os.remove(test_file)
            return False, f"云端内容验证失败: 上传后读回的内容不匹配（本地={expected!r}, 远程={uploaded_content!r}）"

        if upload_proc.returncode != 0:
            logger.info("copyto 退出码非零但内容验证通过，视为验证成功")

        # Cleanup: delete test file from remote
        delete_cmd = f'"{drive_config.rclone_path}" delete "{remote_test_path}"'
        delete_proc = await asyncio.create_subprocess_shell(
            delete_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=_rclone_env()
        )
        await asyncio.wait_for(delete_proc.communicate(), timeout=15)
        # delete failure is non-critical — log but don't block startup

        # Cleanup local test file
        if os.path.exists(test_file):
            os.remove(test_file)

        return True, f"远程存储 {root_remote} 读写验证成功（上传 → 确认 → 删除）"

    except asyncio.TimeoutError:
        if os.path.exists(test_file):
            os.remove(test_file)
        return False, f"Rclone 验证超时（60秒），远程存储 {drive_config.remote_dir} 无响应"
    except FileNotFoundError:
        return False, f"Rclone 可执行文件不存在: {drive_config.rclone_path}"
    except Exception as e:
        if os.path.exists(test_file):
            os.remove(test_file)
        return False, f"Rclone 验证异常: {e}"


async def get_cloud_storage_used(drive_config: CloudDriveConfig) -> Optional[str]:
    """Query OneDrive total storage usage (used / total).

    Returns a human-readable string or None on failure.
    """
    try:
        root = drive_config.remote_dir.split(":")[0] + ":"
        cmd = f'"{drive_config.rclone_path}" about "{root}/"'
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=_rclone_env()
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
        out = stdout.decode(errors="replace") if stdout else ""
        if proc.returncode == 0 and out:
            return out.strip()
    except Exception:
        pass
    return None


class CloudDrive:
    """rclone support"""

    @staticmethod
    def init_upload_adapter(drive_config: CloudDriveConfig):
        """Initialize the upload adapter"""
        if drive_config.upload_adapter == "aligo":
            Aligo = importlib.import_module("aligo").Aligo
            drive_config.aligo = Aligo()

    @staticmethod
    def rclone_mkdir(drive_config: CloudDriveConfig, remote_dir: str):
        """Create directory in remote storage"""
        with Popen(
            f'"{drive_config.rclone_path}" mkdir "{remote_dir}/"',
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=_rclone_env(),
        ):
            pass

    @staticmethod
    def aligo_mkdir(drive_config: CloudDriveConfig, remote_dir: str):
        """Create directory in remote storage via Aligo"""
        if drive_config.aligo and not drive_config.aligo.get_folder_by_path(remote_dir):
            drive_config.aligo.create_folder(name=remote_dir, check_name_mode="refuse")

    @staticmethod
    def zip_file(local_file_path: str) -> str:
        """
        Zip local file
        """

        file_path_without_extension = os.path.splitext(local_file_path)[0]
        zip_file_name = file_path_without_extension + ".zip"

        with ZipFile(zip_file_name, "w") as zip_writer:
            zip_writer.write(local_file_path)

        return zip_file_name

    @staticmethod
    async def rclone_upload_file(
            drive_config: CloudDriveConfig,
            save_path: str,
            local_file_path: str,
            progress_callback: Callable = None,
            progress_args: tuple = (),
    ) -> bool:
        """Use Rclone upload file (copy or move)"""
        try:
            # Build remote directory path
            rel_path = os.path.dirname(local_file_path).replace(save_path, "").lstrip("/\\")
            remote_dir = drive_config.remote_dir.rstrip("/") + "/" + rel_path + "/"
            remote_dir = remote_dir.replace("\\", "/").replace("//", "/")
            logger.info(f"准备上传到远程目录: {remote_dir}")

            # Ensure remote directory exists
            if not drive_config.dir_cache.get(remote_dir):
                CloudDrive.rclone_mkdir(drive_config, remote_dir)
                drive_config.dir_cache[remote_dir] = True

            # Handle compression
            zip_file_path = ""
            file_to_upload = local_file_path
            if drive_config.before_upload_file_zip:
                zip_file_path = CloudDrive.zip_file(local_file_path)
                file_to_upload = zip_file_path
                logger.debug(f"已压缩文件: {zip_file_path}")

            # Choose rclone action
            rclone_action = "move" if drive_config.after_upload_file_delete else "copy"
            # Record local file size before upload for post-upload verification
            local_file_size = os.path.getsize(file_to_upload)
            cmd = (
                f'"{drive_config.rclone_path}" {rclone_action} "{file_to_upload}" '
                f'"{remote_dir}/" --create-empty-src-dirs --progress'
            )
            logger.info(f"执行 rclone 命令: {cmd}")

            proc = await asyncio.create_subprocess_shell(
                cmd, shell=True, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                env=_rclone_env()
            )

            success = False
            transferred = ""
            total = ""
            percent = ""
            speed = ""
            eta = ""

            if proc.stdout:
                async for line_bytes in proc.stdout:
                    line = line_bytes.decode(errors="replace").rstrip()
                    logger.debug(f"rclone stdout: {line}")

                    # 100% progress detected -> success
                    if "100%" in line:
                        success = True

                    # Parse progress info
                    pattern = r"Transferred: (.*?) / (.*?), (.*?)%, (.*?/s)?, ETA (.*?)$"
                    match = re.search(pattern, line)
                    if match:
                        transferred, total, percent, speed, eta = match.groups()
                        if speed is None:
                            speed = "0 B/s"
                        logger.debug(f"进度: {percent}%, 速度: {speed}, 剩余: {eta}")

                        # Call progress callback with full args
                        if progress_callback and progress_args:
                            if len(progress_args) >= 3:
                                node, msg_id, fname = progress_args[0], progress_args[1], progress_args[2]
                                if inspect.iscoroutinefunction(progress_callback):
                                    await progress_callback(transferred, total, percent, speed, eta, node, msg_id,
                                                            fname)
                                else:
                                    await asyncio.get_event_loop().run_in_executor(
                                        None, progress_callback, transferred, total, percent, speed, eta, node, msg_id,
                                        fname
                                    )
                            else:
                                logger.warning(f"progress_args 长度不足: {len(progress_args)}, 期望至少3个")

            # Wait for process to finish
            returncode = await proc.wait()
            stderr = (await proc.stderr.read()).decode(errors="replace") if proc.stderr else ""

            if not success:
                # rclone returned non-zero but we never detected 100% — check stderr for clues
                if returncode != 0:
                    logger.error(f"rclone 进程退出码: {returncode}（未检测到 100% 进度）, stderr: {stderr}")
                    # OneDrive "cancel multipart upload: unauthenticated" is a false-negative;
                    # the file may still have been uploaded successfully — check below.
                else:
                    logger.info(f"rclone 进程正常退出（未检测到 100% 进度）, stderr: {stderr}")
                if rclone_action == "move" and not os.path.exists(file_to_upload):
                    logger.info("使用 move 且源文件已不存在，认为上传成功")
                    success = True
                elif os.path.exists(file_to_upload):
                    logger.error(f"上传失败：未检测到 100% 进度且源文件仍在，{file_to_upload}")
                    return False
                else:
                    logger.warning("未检测到 100% 进度，但进程结束且源文件不在，视为成功")
                    success = True

            # Post-upload: verify remote file actually exists before declaring victory
            if success:
                remote_file_basename = os.path.basename(local_file_path)
                remote_file_path = f"{remote_dir.rstrip('/')}/{remote_file_basename}"

                # 1) Check cloud file exists via rclone size
                logger.debug(f"验证远程文件大小: {remote_file_path}")
                verify_cmd = f'"{drive_config.rclone_path}" size "{remote_file_path}"'
                verify_proc = await asyncio.create_subprocess_shell(
                    verify_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=_rclone_env()
                )
                verify_stdout, verify_stderr = await asyncio.wait_for(verify_proc.communicate(), timeout=15)
                size_output = verify_stdout.decode(errors="replace").strip() if verify_stdout else ""
                logger.debug(f"rclone size 输出: {size_output}")

                # Parse rclone size: "Total size: 230 KiB (235590 Byte)"
                remote_size = -1
                for pattern in [r"\((\d+)\s*Bytes?\)", r"Total size:\s*(\d+)\s*Bytes?"]:
                    m = re.search(pattern, size_output)
                    if m:
                        remote_size = int(m.group(1))
                        break

                cloud_file_ok = remote_size == local_file_size if remote_size != -1 else (
                    verify_proc.returncode == 0
                )

                if not cloud_file_ok and os.path.exists(file_to_upload):
                    logger.error(
                        f"远程文件验证失败: 本地={local_file_size}B, 远程={remote_size}B, {remote_file_path}"
                    )
                    return False

                drive_config.total_upload_success_file_count += 1
                logger.info(f"上传成功: {local_file_path} -> {remote_dir}")

                # 2) Delete local file (rclone move should have deleted it, but be safe)
                if os.path.exists(file_to_upload):
                    try:
                        os.remove(file_to_upload)
                        logger.info(f"删除本地文件: {file_to_upload}")
                    except Exception as e:
                        logger.warning(f"删除本地文件失败: {e}")

                if drive_config.before_upload_file_zip and zip_file_path and os.path.exists(zip_file_path):
                    try:
                        os.remove(zip_file_path)
                    except Exception as e:
                        logger.warning(f"删除压缩文件失败: {e}")

                return True
            else:
                # Upload failed — append to single upload-failure log file
                if os.path.exists(file_to_upload):
                    failed_log = os.path.join(
                        os.path.dirname(file_to_upload),
                        "upload_failed.json"
                    )
                    entry = {
                        "timestamp": datetime.now().isoformat(),
                        "file": local_file_path,
                        "remote_dir": remote_dir,
                        "rclone_action": rclone_action,
                        "rclone_exit_code": returncode,
                        "rclone_stderr": stderr,
                        "local_file_size": os.path.getsize(file_to_upload) if os.path.exists(file_to_upload) else -1
                    }
                    try:
                        records = []
                        if os.path.exists(failed_log):
                            with open(failed_log, "r", encoding="utf-8") as f:
                                try:
                                    records = json.load(f)
                                except:
                                    records = []
                        records.append(entry)
                        with open(failed_log, "w", encoding="utf-8") as f:
                            json.dump(records, f, ensure_ascii=False, indent=2)
                        logger.info(f"上传失败记录已追加: {failed_log}")
                    except Exception as e:
                        logger.warning(f"保存上传失败记录失败: {e}")

                logger.error("上传失败，未达到成功条件")
                return False

        except Exception as e:
            logger.exception(f"rclone_upload_file 异常: {e}")
            return False

    @staticmethod
    def aligo_upload_file(
        drive_config: CloudDriveConfig, save_path: str, local_file_path: str
    ):
        """aliyun upload file"""
        upload_status: bool = False
        if not drive_config.aligo:
            logger.warning("please config aligo! see README.md")
            return False

        try:
            remote_dir = (
                drive_config.remote_dir
                + "/"
                + os.path.dirname(local_file_path).replace(save_path, "")
                + "/"
            ).replace("\\", "/")

            if not drive_config.dir_cache.get(remote_dir):
                CloudDrive.aligo_mkdir(drive_config, remote_dir)
                aligo_dir = drive_config.aligo.get_folder_by_path(remote_dir)
                if aligo_dir:
                    drive_config.dir_cache[remote_dir] = aligo_dir.file_id

            zip_file_path: str = ""
            file_paths = []
            if drive_config.before_upload_file_zip:
                zip_file_path = CloudDrive.zip_file(local_file_path)
                file_paths.append(zip_file_path)
            else:
                file_paths.append(local_file_path)

            res = drive_config.aligo.upload_files(
                file_paths=file_paths,
                parent_file_id=drive_config.dir_cache[remote_dir],
                check_name_mode="refuse",
            )

            if len(res) > 0:
                drive_config.total_upload_success_file_count += len(res)
                if drive_config.after_upload_file_delete:
                    os.remove(local_file_path)

                if drive_config.before_upload_file_zip:
                    os.remove(zip_file_path)

                upload_status = True

        except Exception as e:
            logger.error(f"{e.__class__} {e}")
            return False

        return upload_status

    @staticmethod
    async def upload_file(
        drive_config: CloudDriveConfig, save_path: str, local_file_path: str
    ) -> bool:
        """Upload file
        Parameters
        ----------
        drive_config: CloudDriveConfig
            see @CloudDriveConfig

        save_path: str
            Local file save path config

        local_file_path: str
            Local file path

        Returns
        -------
        bool
            True or False
        """
        if not drive_config.enable_upload_file:
            return False

        ret: bool = False
        if drive_config.upload_adapter == "rclone":
            ret = await CloudDrive.rclone_upload_file(
                drive_config, save_path, local_file_path
            )
        elif drive_config.upload_adapter == "aligo":
            ret = CloudDrive.aligo_upload_file(drive_config, save_path, local_file_path)

        return ret
