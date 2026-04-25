import json
import os
from datetime import datetime
from fractions import Fraction

from PIL import Image as PILImage
from PIL.ExifTags import IFD, TAGS

METADATA_EXTRACTOR_VERSION = 2
PILLOW_METADATA_EXTENSIONS = {".jpg", ".jpeg", ".png", ".tif", ".tiff", ".webp", ".bmp", ".gif"}


def _clean_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8", errors="ignore")
        except Exception:
            return ""
    return str(value).replace("\x00", "").strip()


def _rational_float(value):
    if isinstance(value, tuple) and len(value) == 2:
        denominator = value[1] or 1
        return float(Fraction(value[0], denominator))
    try:
        return float(value)
    except Exception:
        return None


def _format_rational(value, digits: int = 1) -> str:
    numeric = _rational_float(value)
    if numeric is None:
        return ""
    return f"{numeric:.{digits}f}"


def _parse_exif_datetime(value) -> str:
    raw = _clean_text(value)
    if not raw:
        return ""

    if len(raw) > 19 and raw[19:20] in ("+", "-"):
        raw = raw[:19].strip()
    candidates = [raw]
    if len(raw) >= 10 and raw[4] == ":" and raw[7] == ":":
        candidates.append(f"{raw[0:4]}-{raw[5:7]}-{raw[8:10]}{raw[10:]}")

    formats = (
        "%Y:%m:%d %H:%M:%S",
        "%Y:%m:%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%Y:%m:%d",
        "%Y-%m-%d",
    )
    for candidate in candidates:
        trimmed = candidate[:19]
        for fmt in formats:
            try:
                return datetime.strptime(trimmed, fmt).strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
    return raw


def _parse_unix_timestamp(value) -> str:
    try:
        timestamp = int(value)
        if timestamp <= 0:
            return ""
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


def _sidecar_candidates(filepath: str) -> list[str]:
    root, _ext = os.path.splitext(filepath)
    return [
        f"{filepath}.json",
        f"{root}.json",
        f"{filepath}.supplemental-metadata.json",
        f"{root}.supplemental-metadata.json",
    ]


def _apply_sidecar_metadata(metadata: dict, filepath: str) -> None:
    for candidate in _sidecar_candidates(filepath):
        if not os.path.exists(candidate):
            continue
        try:
            with open(candidate, "r", encoding="utf-8") as f:
                sidecar = json.load(f)
        except Exception:
            continue

        taken = (
            sidecar.get("photoTakenTime", {}).get("timestamp")
            or sidecar.get("creationTime", {}).get("timestamp")
        )
        parsed_taken = _parse_unix_timestamp(taken)
        if parsed_taken:
            metadata.setdefault("date_taken", parsed_taken)

        make = _clean_text(sidecar.get("cameraMake") or sidecar.get("camera_make"))
        model = _clean_text(sidecar.get("cameraModel") or sidecar.get("camera_model"))
        if make:
            metadata.setdefault("camera_make", make)
        if model:
            metadata.setdefault("camera_model", model)

        gps = sidecar.get("geoDataExif") or sidecar.get("geoData") or {}
        try:
            lat = float(gps.get("latitude"))
            lng = float(gps.get("longitude"))
            if lat or lng:
                metadata.setdefault("latitude", round(lat, 6))
                metadata.setdefault("longitude", round(lng, 6))
        except Exception:
            pass
        return


def _merge_exif_tags(exif_raw) -> dict:
    all_tags = {}
    if not exif_raw:
        return all_tags

    for tag_id, value in exif_raw.items():
        tag_name = TAGS.get(tag_id, "")
        if tag_name:
            all_tags[tag_name] = value

    for ifd in (IFD.Exif, IFD.GPSInfo):
        try:
            ifd_tags = exif_raw.get_ifd(ifd)
        except Exception:
            ifd_tags = {}
        for tag_id, value in ifd_tags.items():
            tag_name = TAGS.get(tag_id, "")
            if tag_name:
                all_tags[tag_name] = value

    return all_tags


def _format_shutter(value) -> str:
    seconds = _rational_float(value)
    if not seconds or seconds <= 0:
        return ""
    if seconds < 1:
        denominator = round(1 / seconds)
        return f"1/{denominator}"
    if seconds < 10:
        return f"{seconds:.1f}"
    return str(round(seconds))


def _format_exposure_bias(value) -> str:
    numeric = _rational_float(value)
    if numeric is None:
        return ""
    sign = "+" if numeric > 0 else ""
    return f"{sign}{numeric:.1f} EV"


def _dms_to_decimal(dms_tuple, ref: str = ""):
    """Convert GPS DMS (degrees, minutes, seconds) to decimal degrees."""
    try:
        degrees = _rational_float(dms_tuple[0])
        minutes = _rational_float(dms_tuple[1])
        seconds = _rational_float(dms_tuple[2]) if len(dms_tuple) > 2 else 0
        if degrees is None or minutes is None:
            return None
        decimal = degrees + minutes / 60 + (seconds or 0) / 3600
        ref = _clean_text(ref).upper()
        if ref in ("S", "W"):
            decimal = -decimal
        return round(decimal, 6)
    except Exception:
        return None


def extract_image_metadata(filepath: str) -> dict:
    """Return normalized display/query metadata for an image path."""
    filename = os.path.basename(filepath)
    file_ext = os.path.splitext(filename)[1].lower()
    metadata = {
        "filename": filename,
        "filepath": filepath,
        "folder": os.path.dirname(filepath),
        "file_ext": file_ext,
    }

    try:
        stat = os.stat(filepath)
        metadata["file_size"] = int(stat.st_size)
        metadata["filesize"] = f"{stat.st_size / (1024 * 1024):.1f} MB"
        metadata["file_modified_at"] = float(stat.st_mtime)
        metadata["file_modified"] = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass

    _apply_sidecar_metadata(metadata, filepath)
    if file_ext not in PILLOW_METADATA_EXTENSIONS:
        return metadata

    try:
        with PILImage.open(filepath) as img:
            metadata["width"] = int(img.width)
            metadata["height"] = int(img.height)
            metadata["dimensions"] = f"{img.width} x {img.height}"
            exif_raw = img.getexif()
            all_tags = _merge_exif_tags(exif_raw)
    except Exception:
        return metadata

    make = _clean_text(all_tags.get("Make"))
    model = _clean_text(all_tags.get("Model"))
    if make and model.startswith(make):
        model = model[len(make):].strip()
    if make:
        metadata["camera_make"] = make
    if model:
        metadata["camera_model"] = model

    lens = _clean_text(all_tags.get("LensModel") or all_tags.get("LensMake"))
    if lens:
        metadata["lens"] = lens

    focal_length = _rational_float(all_tags.get("FocalLength"))
    if focal_length is not None:
        metadata["focal_length"] = f"{focal_length:.0f}mm"

    focal_length_35mm = all_tags.get("FocalLengthIn35mmFilm")
    if focal_length_35mm is not None:
        try:
            metadata["focal_length_35mm"] = f"{int(focal_length_35mm)}mm"
        except Exception:
            pass

    f_number = _format_rational(all_tags.get("FNumber"), 1)
    if f_number:
        metadata["aperture"] = f"f/{f_number}"

    shutter = _format_shutter(all_tags.get("ExposureTime"))
    if shutter:
        metadata["shutter_speed"] = shutter

    iso = all_tags.get("ISOSpeedRatings") or all_tags.get("PhotographicSensitivity")
    if iso is not None:
        if isinstance(iso, (tuple, list)):
            iso = iso[0] if iso else ""
        try:
            metadata["iso"] = str(int(iso))
        except Exception:
            metadata["iso"] = _clean_text(iso)

    exp_prog_map = {1: "Manual", 2: "Program", 3: "Aperture Priority", 4: "Shutter Priority"}
    exp_prog = all_tags.get("ExposureProgram")
    if exp_prog in exp_prog_map:
        metadata["exposure_program"] = exp_prog_map[exp_prog]

    bias = _format_exposure_bias(all_tags.get("ExposureBiasValue"))
    if bias:
        metadata["exposure_bias"] = bias

    metering_map = {
        2: "Center-weighted",
        3: "Spot",
        5: "Pattern",
        6: "Partial",
    }
    metering = all_tags.get("MeteringMode")
    if metering in metering_map:
        metadata["metering_mode"] = metering_map[metering]

    white_balance = all_tags.get("WhiteBalance")
    if white_balance is not None:
        metadata["white_balance"] = "Auto" if white_balance == 0 else "Manual"

    flash = all_tags.get("Flash")
    if flash is not None:
        try:
            metadata["flash"] = "Fired" if (int(flash) & 1) else "No flash"
        except Exception:
            pass

    raw_date = (
        all_tags.get("DateTimeOriginal")
        or all_tags.get("DateTimeDigitized")
        or all_tags.get("DateTime")
    )
    if raw_date:
        parsed = _parse_exif_datetime(raw_date)
        metadata["date"] = _clean_text(raw_date)
        if parsed:
            metadata["date_taken"] = parsed

    width = all_tags.get("ExifImageWidth") or all_tags.get("ImageWidth")
    height = all_tags.get("ExifImageHeight") or all_tags.get("ImageLength")
    if width and height:
        try:
            metadata["width"] = int(width)
            metadata["height"] = int(height)
            metadata["dimensions"] = f"{int(width)} x {int(height)}"
        except Exception:
            pass

    # GPS coordinates
    gps_lat = all_tags.get("GPSLatitude")
    gps_lat_ref = all_tags.get("GPSLatitudeRef")
    gps_lng = all_tags.get("GPSLongitude")
    gps_lng_ref = all_tags.get("GPSLongitudeRef")
    if gps_lat and gps_lng:
        lat = _dms_to_decimal(gps_lat, gps_lat_ref)
        lng = _dms_to_decimal(gps_lng, gps_lng_ref)
        if lat is not None and lng is not None:
            metadata["latitude"] = lat
            metadata["longitude"] = lng

    return metadata
