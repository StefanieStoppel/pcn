import json


def create_list_files_for_data_partitions(input_file="../ycb_data_tmp/data_new.json",
                                          output_dir="../ycb_data_tmp"):
    """
    Creates train.list, val.list and test.list files from the json files containing the train / val / test split
    of the YCB objects.
    :param input_file: .json file path
    :param output_dir: directory to save output .list files to
    """
    with open(input_file, "r") as json_data:
        data = json.load(json_data)
        for data_partition_key in data.keys():
            joined_partition_video_frames = \
                sorted([item.split(".pcd")[0] for sublist in data[data_partition_key].values() for item in sublist])
            save_list_file(data_partition_key, joined_partition_video_frames, output_dir)


def save_list_file(key, iterable, output_dir):
    with open(f"{output_dir}/{key}.list", "w") as list_file:
        for item in iterable:
            list_file.write("%s\n" % item)


if __name__ == "__main__":
    create_list_files_for_data_partitions()