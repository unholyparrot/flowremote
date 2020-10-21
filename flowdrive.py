#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Скрипт для загрузки файлов на GoogleDrive и их выгрузки оттуда. 

Пока сделано достаточно костыльно, но этого пока вроде достаточно. 
Большая благодарность http://datalytics.ru/all/rabotaem-s-api-google-drive-s-pomoschyu-python/
Основная часть кода была взята оттуда.
"""


import argparse
import json
import datetime
import io
import os

from tqdm.auto import tqdm
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload,MediaFileUpload
from googleapiclient.discovery import build


__author__ = "unholyparrot"
__version__ = "0.1.0"


def setup_args():
    parser = argparse.ArgumentParser(description="Download or upload from/to GoogleDrive")

    parser.add_argument('-person',
                        '--person',
                        help="Name of your folder", 
                        type=str,
                        required=True)

    parser.add_argument('-up', 
                        '--upload',
                        nargs='+',
                        help="""Upload files on GoogleDrive. 
                                Multiply files or single, but only files, not folders. 
                                Example: -up /home/owl/text.txt /home/owl/tost.doc""",
                        type=str,
                        required=False)
    
    parser.add_argument('-down', 
                        '--download',
                        nargs='+',
                        help="""Download files from GoogleDrive. 
                                Multiply files or single, but only files, not folders.
                                Pay attention to names on GoogleDrive. 
                                Example: -down up_text.txt up_tost.doc 
                                You can use key "all" to download all files from your folder.""",
                        type=str,
                        required=False)
    
    parser.add_argument('-rm', 
                        '--remove',
                        help="""Clears up file(s) from your folder on GoogleDrive. 
                                Multiply files or single, but only files, not folders.
                                Pay attention to names on GoogleDrive.  
                                Example: -rm up_text.txt upp_tost.doc
                                You can use key "all" to clear up everything from your folder.""",
                        type=str,
                        required=False)
    
    return parser.parse_args()


def main():
    args = setup_args()
    with open('log_files/log_flowdrive.txt', 'a+') as logger:
        logger.write(str(datetime.datetime.now()) + ' | argparse | {}'.format(args) + '\n')

    # проверка пользователя
    with open("config_files/flowdrive_persons.json", "r") as read_file:
        persons_data = json.load(read_file)
    if persons_data.get(args.person):
        folder_id = persons_data.get(args.person)['folder_id']
        folder_path = persons_data.get(args.person)['folder_path']
    else:
        print("Person '{}' is not included in persons list. Check the spelling.".format(args.person))
        with open('log_files/log_flowdrive.txt', 'a+') as logger:
            logger.write("__________" + '\n')
        exit(1)
    
    # проверка на количество введённых аргументов
    if ((int(bool(args.upload)) + int(bool(args.download)) + int(bool(args.remove))) > 1):
        print("Choose not more than one option.")
        with open('log_files/log_flowdrive.txt', 'a+') as logger:
            logger.write("__________" + '\n')
        exit(1)
    
    # настройка параметров подключения к диску
    SCOPES = ['https://www.googleapis.com/auth/drive']
    SERVICE_ACCOUNT_FILE = 'config_files/flowdrive_remotedrivekey.json'
    with open("config_files/flowdrive_remotedrivekey.json", "r") as read_file:
            SERVICE_ACCOUNT_MAIL = json.load(read_file)['client_email']
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('drive', 'v3', credentials=credentials)

    # если производим загрузку файлов на диск 
    if args.upload:
        with open("log_files/log_flowdrive.txt", 'a+') as logger:
            for f_name in tqdm(args.upload, desc='Upload process'):
                name = "up_" + os.path.split(f_name)[1]
                # добавляем проверку на то, что это файл, а не папка
                if name == "up_":
                    print("Can't process folders yet. (file name {})".format(f_name))
                    continue
                else:
                    file_path = f_name
                    file_metadata  = {
                                    'name': name,
                                    'parents': [folder_id]
                                    }
                    media = MediaFileUpload(file_path, resumable=True)
                    r = service.files().create(body=file_metadata, media_body=media, fields='id').execute()  # pylint: disable=no-member
                    logger.write(str(datetime.datetime.now()) + " | upload | id : " + r['id'] + '\n')

    # если производим скачивание файлов с диска         
    if args.download:
        with open("log_files/log_flowdrive.txt", 'a+') as logger:
            results = service.files().list(  # pylint: disable=no-member
                fields="files(id, name, mimeType, parents, createdTime)", 
                q="'{}' in owners".format(SERVICE_ACCOUNT_MAIL)).execute()
            if (len(args.download) == 1) and (args.download[0] == "all"):
                for elem in results['files']:
                    if elem['parents'][0] == folder_id:
                        curr_id = elem['id']
                        request = service.files().get_media(fileId=curr_id)  # pylint: disable=no-member
                        filename = folder_path + elem['name']
                        fh = io.FileIO(filename, 'wb')
                        downloader = MediaIoBaseDownload(fh, request)
                        done = False
                        while done is False:
                            status, done = downloader.next_chunk()
                            print("Download %d%%." % int(status.progress() * 100))
                        logger.write(str(datetime.datetime.now()) + " | download | id : " + curr_id + '\n')
            else:
                for elem in results['files']:
                    if (elem['parents'][0] == folder_id) and (elem['name'] in args.download):
                        curr_id = elem['id']
                        request = service.files().get_media(fileId=curr_id)  # pylint: disable=no-member
                        filename = folder_path + elem['name']
                        fh = io.FileIO(filename, 'wb')
                        downloader = MediaIoBaseDownload(fh, request)
                        done = False
                        while done is False:
                            status, done = downloader.next_chunk()
                            print("Download %d%%." % int(status.progress() * 100))
                        logger.write(str(datetime.datetime.now()) + " | download | id : " + curr_id + '\n')
    
    if args.remove:
        with open("log_files/log_flowdrive.txt", 'a+') as logger:
            results = service.files().list(  # pylint: disable=no-member
                fields="files(id, name, mimeType, parents, createdTime)", 
                q="'{}' in owners".format(SERVICE_ACCOUNT_MAIL)).execute()
            
            remove_flag = False
            if args.remove == "all":
                for elem in results['files']:
                    if elem['parents'][0] == folder_id:
                        curr_id = elem['id']
                        service.files().delete(fileId=curr_id).execute()  # pylint: disable=no-member
                        print("{} is removed.".format(args.remove))
                        logger.write(str(datetime.datetime.now()) + " | remove | id : " + curr_id + '\n')
                remove_flag = True
            else:
                for elem in results['files']:
                    if (elem['parents'][0] == folder_id) and (elem['name'] == args.remove):
                        remove_flag = True
                        curr_id = elem['id']
                        service.files().delete(fileId=curr_id).execute()  # pylint: disable=no-member
                        print("{} is removed.".format(args.remove))
                        logger.write(str(datetime.datetime.now()) + " | remove | id : " + curr_id + '\n')
            if not remove_flag:
                print("{} is not found on Drive.".format(args.remove))


if __name__ == "__main__":
    main()
    with open('log_files/log_flowdrive.txt', 'a+') as logger:
        logger.write("__________" + '\n')
