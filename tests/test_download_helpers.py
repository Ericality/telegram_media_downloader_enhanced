"""Tests for download helper functions (_can_download / _is_exist / _check_timeout / _check_download_finish / _move_to_download_path)."""
from unittest import mock

import pyrogram

import media_downloader as md


def test_can_download_all():
    assert md._can_download("video", {"video": ["all"]}, "mp4") is True


def test_can_download_matching_format():
    assert md._can_download("video", {"video": ["mp4"]}, "mp4") is True


def test_can_download_unmatched_format():
    assert md._can_download("video", {"video": ["mp4"]}, "avi") is False


def test_can_download_non_restricted_type():
    assert md._can_download("photo", {"video": ["mp4"]}, None) is True


def test_is_exist_file(tmp_path):
    f = tmp_path / "x.mp4"
    f.write_text("x")
    assert md._is_exist(str(f)) is True


def test_is_exist_directory(tmp_path):
    d = tmp_path / "sub"
    d.mkdir()
    assert md._is_exist(str(d)) is False


def test_is_exist_missing(tmp_path):
    assert md._is_exist(str(tmp_path / "nope.mp4")) is False


def test_check_timeout():
    assert md._check_timeout(2, 0) is True
    assert md._check_timeout(0, 0) is False
    assert md._check_timeout(1, 0) is False


def test_check_download_finish_size_matches():
    with mock.patch("media_downloader.os.path.getsize", return_value=100) as getsize, \
            mock.patch("media_downloader.os.remove") as os_remove:
        md._check_download_finish(100, "/tmp/x.mp4", "x.mp4")
        getsize.assert_called_once_with("/tmp/x.mp4")
        os_remove.assert_not_called()


def test_check_download_finish_size_mismatch_raises():
    with mock.patch("media_downloader.os.path.getsize", return_value=50), \
            mock.patch("media_downloader.os.remove") as os_remove:
        try:
            md._check_download_finish(100, "/tmp/x.mp4", "x.mp4")
            assert False, "expected BadRequest to be raised"
        except pyrogram.errors.exceptions.bad_request_400.BadRequest:
            pass
        os_remove.assert_called_once_with("/tmp/x.mp4")


def test_move_to_download_path():
    with mock.patch("media_downloader.os.makedirs") as makedirs, \
            mock.patch("media_downloader.shutil.move") as move:
        md._move_to_download_path("/tmp/a.mp4", "/final/dir/b.mp4")
        makedirs.assert_called_once_with("/final/dir", exist_ok=True)
        move.assert_called_once_with("/tmp/a.mp4", "/final/dir/b.mp4")
