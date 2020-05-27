# Author: Wentao Yuan (wyuan1@cs.cmu.edu) 05/31/2018, Stefanie Stoppel 05/27/2020

import argparse
import os
from io_util import read_pcd
from tensorpack import DataFlow, dataflow


class pcd_df(DataFlow):
    def __init__(self, model_list, num_scans, partial_dir, complete_dir):
        self.model_list = model_list
        self.num_scans = num_scans
        self.partial_dir = partial_dir
        self.complete_dir = complete_dir

    def size(self):
        return len(self.model_list) * self.num_scans

    def get_data(self):
        for model_id in self.model_list:
            complete = read_pcd(os.path.join(self.complete_dir, '%s.pcd' % model_id))
            for i in range(self.num_scans):
                partial = read_pcd(os.path.join(self.partial_dir, model_id, '%d.pcd' % i))
                yield model_id.replace('/', '_'), partial, complete


class YCBPointCloudDataFlow(pcd_df):
    def __init__(self, model_list, partial_dir, complete_dir, partial_ext=".pcd", complete_ext=".pcd"):
        # our model_list for YCB contains all the point cloud files, so always one model per list item
        num_scans = 1
        super().__init__(model_list, num_scans, partial_dir, complete_dir)
        self.partial_ext = partial_ext
        self.complete_ext = complete_ext
        self.complete_pc_dict = self.get_complete_pc_dict()

    def get_complete_pc_dict(self):
        complete_pc_dict = {}
        for pc_file in os.listdir(self.complete_dir):
            ycb_object_name, ext = os.path.splitext(os.path.basename(pc_file))
            if ext == self.complete_ext:
                complete_pc_dict[ycb_object_name] = read_pcd(os.path.join(self.complete_dir, pc_file))
        return complete_pc_dict

    def get_data(self):
        for model_id in self.model_list:
            model_name = self.get_model_name(model_id)
            if model_name not in self.complete_pc_dict:
                continue
            complete = self.complete_pc_dict[model_name]
            for i in range(self.num_scans):
                partial = read_pcd(os.path.join(self.partial_dir, f"{model_id}{self.partial_ext}"))
                yield model_id, partial, complete

    @staticmethod
    def get_model_name(model_id: str):
        return model_id.split("-")[1]


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--use_ycb_data', dest='use_ycb_data', action='store_true')
    parser.add_argument('--list_path')
    parser.add_argument('--num_scans', type=int, default=1)
    parser.add_argument('--partial_dir')
    parser.add_argument('--complete_dir')
    parser.add_argument('--output_path')
    args = parser.parse_args()

    with open(args.list_path) as file:
        model_list_ = file.read().splitlines()
    if args.use_ycb_data:
        data_flow = YCBPointCloudDataFlow(model_list_, args.partial_dir, args.complete_dir)
    else:
        data_flow = pcd_df(model_list_, args.num_scans, args.partial_dir, args.complete_dir)
    if os.path.exists(args.output_path):
        os.system('rm %s' % args.output_path)
    dataflow.LMDBSerializer.save(data_flow, args.output_path)
