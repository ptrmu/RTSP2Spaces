import argparse
import boto3
import cv2
import datetime as dt
import json
import pathlib as pl
import types


class StdMsg:
    def __init__(self):
        self.app_name = pl.Path(__file__).stem
        self.now_exec_local = dt.datetime.now()

    def __call__(self, error_exit, msg):
        print(f"{self.app_name} {self.now_exec_local.strftime("%m/%d/%Y-%H:%M:%S")} " +
              ("ERROR: " if error_exit else ": ") + msg)
        if error_exit:
            exit(1)


def std_msgx(now_exec_local, error_exit, msg):
    print(f"{now_exec_local.strftime("%m/%d/%Y-%H:%M:%S")} " +
          ("ERROR: " if error_exit else ": ") + msg)
    if error_exit:
        exit(1)


class CustomArgumentParser(argparse.ArgumentParser):
    def __init__(self, *args, std_msg, **kwargs):
        super().__init__(*args, **kwargs)
        self.std_msg = std_msg  # Store the extra information

    def error(self, message):
        self.std_msg(True, message)


def parse_args(std_msg):
    parser = CustomArgumentParser(std_msg=std_msg)

    parser.add_argument("secrets_filename",
                        type=str,
                        help="JSON file with secrets: rtsp_user, rtsp_pw, spaces_key, spaces_access_key")

    parser.add_argument("--host",
                        type=str,
                        help="rtsp host name",
                        default="192.168.1.25")
    parser.add_argument("--stream_selector",
                        type=str,
                        help="rtsp stream name/selector",
                        default="Preview_02_main")

    parser.add_argument("--region",
                        type=str,
                        help="spaces region name",
                        default="sfo3")
    parser.add_argument("--bucket",
                        type=str,
                        help="spaces bucket name",
                        default="stake-images")
    parser.add_argument("--upload_image_name",
                        type=str,
                        help="spaces name for uploaded image file",
                        default="mazama/latest/stake_image.jpg")
    parser.add_argument("--upload_metadata_name",
                        type=str,
                        help="spaces name for uploaded metadata file",
                        default="mazama/latest/stake_image.json")

    parser.add_argument("--image_name_prefix",
                        help="local image filename prefix",
                        default='files/mazama_stake_')
    parser.add_argument("--metadata_name",
                        help="local metadata filename",
                        default='mazama_stake')
    return parser.parse_args()


def load_secrets(std_msg, args):
    if not pl.Path(args.secrets_filename).exists():
        std_msg(True, f"Secrets file not found at: {args.secrets_filename}")

    with open(args.secrets_filename, 'r') as file:
        secrets = json.load(file)

    # Validate that all required keys exist
    for key in ["rtsp_user", "rtsp_pw", "spaces_key", "spaces_access_key"]:
        if key not in secrets:
            std_msg(True, f"{key} is missing from the secrets file.")

    return types.SimpleNamespace(**secrets)


def capture_image(std_msg, args, secrets):
    # Create a VideoCapture object
    rtsp_url = f"rtsp://{secrets.rtsp_user}:{secrets.rtsp_pw}@{args.host}/{args.stream_selector}"
    cap = cv2.VideoCapture(rtsp_url)

    # Check if the camera opened successfully
    if not cap.isOpened():
        std_msg(True, f"Can not open stream:{rtsp_url}")

    ret0 = cap.grab()
    ret1 = cap.grab()
    ret2 = cap.grab()

    time_utc = dt.datetime.now(dt.timezone.utc)
    time_local = dt.datetime.now()

    ret3, frame = cap.retrieve()

    # Release the capture
    cap.release()

    if not (ret0 and ret1 and ret2 and ret3):
        std_msg(True, "Can't receive frame")

    return types.SimpleNamespace(frame=frame,
                                 time_utc=time_utc,
                                 time_local=time_local)


def create_filenames(args, capture):
    capture_local_filename = capture.time_local.strftime("%Y%m%d%H%M")
    script_dir = pl.Path(__file__).resolve().parent

    return types.SimpleNamespace(
        local_image=script_dir.joinpath(args.image_name_prefix + capture_local_filename + ".jpg"),
        local_metadata=script_dir.joinpath(args.metadata_name + ".json"),
        spaces_image_url=f"https://{args.bucket}.{args.region}.digitaloceanspaces.com/{args.upload_image_name}",
        spaces_metadata_url=f"https://{args.bucket}.{args.region}.digitaloceanspaces.com/{args.upload_metadata_name}",
        spaces_endpoint_url=f"https://{args.region}.digitaloceanspaces.com"
    )


def save_image(filenames, capture):
    shape = capture.frame.shape
    height = shape[0]
    width = shape[1]
    new_width = height if height < width else width
    origin = max(0, int(width / 2.0 - new_width / 2.0))

    cv2.imwrite(str(filenames.local_image), capture.frame[:, origin:origin + new_width])


def save_metadata(filenames, capture):
    metadata = {
        "image": {
            "url": filenames.spaces_image_url,
            "alt": "Mazama Snow Stake"
        },
        "timestamp": int(capture.time_utc.timestamp()),
        "valid_until": int((capture.time_utc + dt.timedelta(seconds=300)).timestamp())
    }
    with open(filenames.local_metadata, 'w') as json_file:
        json.dump(metadata, json_file, indent=4)


def upload_to_spaces(std_msg, args, secrets, filenames):
    client = boto3.client('s3',
                          region_name=args.region,
                          endpoint_url=filenames.spaces_endpoint_url,
                          aws_access_key_id=secrets.spaces_key,
                          aws_secret_access_key=secrets.spaces_access_key)

    try:
        client.upload_file(filenames.local_image, args.bucket, args.upload_image_name,
                           ExtraArgs={"ContentType": "image/jpeg",
                                      "CacheControl": "max-age=60",
                                      "ACL": "public-read"})

    except Exception as e0:
        std_msg(True, f"Error occurred uploading image. Check Space name, etc ({e0})")

    try:
        client.upload_file(filenames.local_metadata, args.bucket, args.upload_metadata_name,
                           ExtraArgs={"ContentType": "application/json",
                                      "CacheControl": "max-age=60",
                                      "ACL": "public-read"})

    except Exception as e1:
        std_msg(True, f"Error occurred uploading metadata. Check Space name, etc ({e1})", e1)
        exit(1)

    client.close()
    std_msg(False, "Success")  # A threading crash happens more often if this msg is moved out of this method


def main():
    std_msg = StdMsg()
    args = parse_args(std_msg)
    secrets = load_secrets(std_msg, args)
    capture = capture_image(std_msg, args, secrets)
    filenames = create_filenames(args, capture)
    save_image(filenames, capture)
    save_metadata(filenames, capture)
    upload_to_spaces(std_msg, args, secrets, filenames)


main()

#
# # Create a VideoCapture object
# cap = cv2.VideoCapture("rtsp://admin:FnJmtZsAsZ9.@192.168.1.25/Preview_02_main")
#
# # Check if the camera opened successfully
# if not cap.isOpened():
#     print("Cannot open camera")
#     exit()
#
# ret0 = cap.grab()
# ret1 = cap.grab()
# ret2 = cap.grab()
#
# now_utc = dt.datetime.now(dt.timezone.utc)
# now_local = dt.datetime.now()
#
# ret3, frame = cap.retrieve()
#
# # Release the capture
# cap.release()
#
# now_local_str = now_local.strftime("%m/%d/%Y-%H:%M:%S")
# now_local_filename = now_local.strftime("%Y%m%d%H%M")
#
# if not (ret0 and ret1 and ret2 and ret3):
#     print(now_local_str, "Can't receive frame (stream end?). Exiting ...")
#     exit(1)
#
# # Find where this script is running from
# script_dir = pl.Path(__file__).resolve().parent
# local_file = script_dir.joinpath("stake_image.jpg")
# save_file = script_dir.joinpath("files", "mazama_stake_" + now_local_filename + ".jpg")
# metadata_file = script_dir.joinpath("stake_image.json")
#
# shape = frame.shape
# height = shape[0]
# width = shape[1]
# new_width = height if height < width else width
# origin = max(0, int(width / 2.0 - new_width / 2.0))
#
# # Save the image and make a copy with a unique filename
# cv2.imwrite(str(local_file), frame[:, origin:origin+new_width])
# shutil.copy2(local_file, save_file)
#
# key_id = str("DO00KD6F2YQCMVAWFF8N")
# secret_access_key = str("bategMqsRz+Ht4TKxPeVGleJII4C/n3VZLsC0kDxxDQ")
# region_name = "sfo3"
# bucket_name = "stake-images"
# image_upload_name = "mazama/latest/stake_image.jpg"
# metadata_upload_name = "mazama/latest/stake_image.json"
#
# endpoint_url = 'https://' + str(region_name) + '.digitaloceanspaces.com'
# image_base_url = 'https://' + bucket_name + "." + str(region_name) + '.digitaloceanspaces.com'
#
# # Create the metadata file
# metadata = {
#     "image": {
#         "url": image_base_url + "/" + image_upload_name,
#         "alt": "Mazama Snow Stake"
#     },
#     "timestamp": int(now_utc.timestamp()),
#     "valid_until": int((now_utc + dt.timedelta(seconds=300)).timestamp())
# }
# with open(metadata_file, 'w') as json_file:
#     json.dump(metadata, json_file, indent=4)
#
# # Upload the image and metadata files to spaces
# session = boto3.session.Session()
# client = session.client('s3',
#                         region_name=str(region_name),
#                         endpoint_url=endpoint_url,
#                         aws_access_key_id=key_id,
#                         aws_secret_access_key=secret_access_key)
#
#
# try:
#     client.upload_file(local_file, bucket_name, image_upload_name,
#                        ExtraArgs={"ContentType": "image/jpeg", "CacheControl": "max-age=60", "ACL": "public-read"})
#
# except Exception as e:
#     print(now_local_str, "Error occurred uploading image. Check Space name, etc", e)
#     exit(1)
#
# try:
#     client.upload_file(metadata_file, bucket_name, metadata_upload_name,
#                        ExtraArgs={"ContentType": "application/json", "CacheControl": "max-age=60", "ACL": "public-read"})
#
# except Exception as e:
#     print(now_local_str, "Error occurred uploading metadata. Check Space name, etc", e)
#     exit(1)
#
# print(now_local_str, "Success")
#
