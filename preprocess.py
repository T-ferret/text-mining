import os
import zipfile
import json
import pandas as pd
from tqdm import tqdm


def load_raw_from_zip(raw_zip_path):
    all_sheets = []

    with zipfile.ZipFile(raw_zip_path, 'r') as z:
        for member in z.namelist():
            if not member.lower().endswith(('.xlsx', 'xls')):
                continue
            with z.open(member) as f:
                try:
                    df_sheet = pd.read_excel(f)
                except Exception as e:
                    print(f'{member} 읽기 실패: {e}')
                    continue

                if "INDEX" in df_sheet.columns:
                    df_sheet["INDEX"] = df_sheet["INDEX"].astype(int, errors='ignore')
                all_sheets.append(df_sheet)

    if not all_sheets:
        return pd.DataFrame()

    df_raw = pd.concat(all_sheets, ignore_index=True)

    return df_raw


# def load_paired_zip(raw_zip_path, label_zip_path):
#     """
#     :param raw_zip_path: “01.원천데이터” 폴더 안의 ZIP 경로
#     :param label_zip_path: “02.라벨링데이터” 폴더 안의 동일 속성 ZIP 경로
#     :return: 두 ZIP 내부 JSON을 Index 기준으로 병합해서 하나의 DataFrame으로 반환
#     """
#
#     raw_records = {}
#     label_records = {}
#
#     # 원천데이터 zip 내부 json 읽기
#     with zipfile.ZipFile(raw_zip_path, 'r') as z_raw:
#         for member in z_raw.namelist():
#             if not member.lower().endswith('.json'):
#                 continue
#             with z_raw.open(member) as f:
#                 data_list = json.load(f)
#                 for entry in data_list:
#                     idx = entry.get('index')
#                     raw_records[idx] = {
#                         'RawText': entry.get('RawText', ''),
#                         'GeneralPolarity_raw': int(entry.get('GeneralPolarity_raw', 0)),
#                         'Aspects_raw': entry.get('Aspects', []),
#                     }
#     # 라벨링데이터 zip 내부 json 읽기
#     with zipfile.ZipFile(label_zip_path, 'r') as z_label:
#         for member in z_label.namelist():
#             if not member.lower().endswith('.json'):
#                 continue
#             with z_label.open(member) as f:
#                 data_list = json.load(f)
#                 for entry in data_list:
#                     idx = entry.get('index')
#                     label_records[idx] = {
#                         "GeneralPolarity_raw": int(entry.get('GeneralPolarity_raw', 0)),
#                         "Aspects_raw": entry.get('Aspects', []),
#                     }
#
#     # 두 딕셔너리의 키(index)를 기준으로 병합
#     merged_rows = []
#     for idx, raw_info in raw_records.items():
#         if idx not in label_records:
#             # 라벨이 없는 경우 건너뜀
#             continue
#
#         label_info = label_records[idx]
#         row = {
#             'Index': idx,
#             'RawText': raw_info['RawText'],
#             'GeneralPolarity_raw': raw_info['GeneralPolarity_raw'],
#             'Aspects_raw': raw_info['Aspects_raw'],
#             'GeneralPolarity_label': label_info['GeneralPolarity_label'],
#             'Aspects_label': label_info['Aspects_label'],
#         }
#         merged_rows.append(row)
#
#     df = pd.DataFrame(merged_rows)
#     return df


def load_all_attributes(base_dir, split='Training'):
    raw_dir = os.path.join(base_dir, split, '01.원천데이터')
    label_dir = os.path.join(base_dir, split, '02.라벨링데이터')

    all_dfs = []
    for fname in os.listdir(raw_dir):
        if not fname.lower().endswith('.zip'):
            continue
        raw_zip_path = os.path.join(raw_dir, fname)
        label_zip_path = os.path.join(label_dir, fname)

        if not os.path.exists(label_zip_path):
            print(f'Warning: {fname}에대한 라벨링 zip 파일을 찾을 수 업음. 건너뜀.')
            continue

        try:
            attr_name = fname.split('.')[-2]
        except:
            attr_name = fname[:-4]

        print(f'속성: {attr_name}')
        print(f'원천: {raw_zip_path}, 라벨: {label_zip_path}')
        df_attr = load_paired_zip(raw_zip_path, label_zip_path)
        df_attr['Attribute'] = attr_name
        all_dfs.append(df_attr)

    if all_dfs:
        df_all = pd.concat(all_dfs, ignore_index=True)
    else:
        df_all = pd.DataFrame()

    return df_all


