# -*- coding:utf-8 -*-
from app.helpers.xiami_helper import get_promotion_albums
import time
import MySQLdb


class WorkWork():
    def __init__(self, limit):
        self.host = '101.200.159.42'
        self.user = 'java'
        self.pw = 'inspero'
        self.database = 'musicnew'
        self.LIMIT_PAGE = limit
        self.time_stamp = []
        self.database = MySQLdb.connect(self.host, self.user, self.pw, self.database, charset='utf8')
        self.cursor = self.database.cursor()
        self.cursor.execute('select version()')
        data = self.cursor.fetchone()
        print int(time.time()), 'Database version : %s' % data
        del data

    def get_first(self, albums):
        '''
        :param albums: all albums that can be inserted into mysql
        :return: the list contains the data to be inserted into mysql
        '''
        data_list = []

        for album in albums:
            try:
                is_play = str(album['is_play'])
            except:
                is_play = None
            try:
                album_id = str(album['album_id'])
            except:
                album_id = None
            try:
                grade = str(album['grade'])
            except:
                grade = None
            try:
                is_exclusive = str(album['is_exclusive'])
            except:
                is_exclusive = None
            try:
                collects = str(album['collects'])
            except:
                collects = None
            try:
                logo = str(album['logo'])
            except:
                logo = None
            try:
                category = str(album['category'])
            except:
                category = None
            try:
                song_count = str(album['song_count'])
            except:
                song_count = None
            try:
                sub_title = str(album['sub_title'].encode('utf-8'))
            except:
                sub_title = None
            try:
                artist_id = str(album['artist_id'])
            except:
                artist_id = None
            try:
                status = str(album['status'])
            except:
                status = None
            try:
                recommends = str(album['recommends'])
            except:
                recommends = None
            try:
                description = str(album['description'].encode('utf-8'))
            except:
                description = None
            try:
                check_rate = str(album['check_rate'].encode('utf-8'))
            except:
                check_rate = None
            try:
                is_check = str(album['is_check'])
            except:
                is_check = None
            try:
                play_count = str(album['play_count'])
            except:
                play_count = None
            try:
                gmt_publish = str(album['gmt_publish'])
            except:
                gmt_publish = None
            try:
                cd_count = str(album['cd_count'])
            except:
                cd_count = None
            try:
                artist_name = str(album['artist_name'].encode('utf-8'))
            except:
                artist_name = None
            try:
                company = str(album['company'].encode('utf-8'))
            except:
                company = None
            try:
                language = str(album['language'].encode('utf-8'))
            except:
                language = None
            try:
                album_name = str(album['album_name'].encode('utf-8'))
            except:
                album_name = None
            insert_timestamp = str(int(time.time()))
            data_list.append((insert_timestamp, is_play, album_id, grade, is_exclusive, collects, logo, category,
                              song_count, sub_title,
                              artist_id, status, recommends, description, check_rate, is_check, play_count,
                              gmt_publish, cd_count, artist_name, company, language, album_name))
        return data_list

    def get_new(self, albums):
        '''
        :param albums: all albums that can be inserted into mysql
        :return: the list contains the data to be inserted into mysql
        '''
        data_list = []

        for album in albums:
            try:
                is_play = str(album['is_play'])
            except:
                is_play = None
            try:
                album_id = str(album['album_id'])
            except:
                album_id = None
            try:
                grade = str(album['grade'])
            except:
                grade = None
            try:
                is_exclusive = str(album['is_exclusive'])
            except:
                is_exclusive = None
            try:
                collects = str(album['collects'])
            except:
                collects = None
            try:
                logo = str(album['logo'])
            except:
                logo = None
            try:
                category = str(album['category'])
            except:
                category = None
            try:
                song_count = str(album['song_count'])
            except:
                song_count = None
            try:
                sub_title = str(album['sub_title'].encode('utf-8'))
            except:
                sub_title = None
            try:
                artist_id = str(album['artist_id'])
            except:
                artist_id = None
            try:
                status = str(album['status'])
            except:
                status = None
            try:
                recommends = str(album['recommends'])
            except:
                recommends = None
            try:
                description = str(album['description'].encode('utf-8'))
            except:
                description = None
            try:
                check_rate = str(album['check_rate'].encode('utf-8'))
            except:
                check_rate = None
            try:
                is_check = str(album['is_check'])
            except:
                is_check = None
            try:
                play_count = str(album['play_count'])
            except:
                play_count = None
            try:
                gmt_publish = str(album['gmt_publish'])
            except:
                gmt_publish = None
            try:
                cd_count = str(album['cd_count'])
            except:
                cd_count = None
            try:
                artist_name = str(album['artist_name'].encode('utf-8'))
            except:
                artist_name = None
            try:
                company = str(album['company'].encode('utf-8'))
            except:
                company = None
            try:
                language = str(album['language'].encode('utf-8'))
            except:
                language = None
            try:
                album_name = str(album['album_name'].encode('utf-8'))
            except:
                album_name = None
            insert_timestamp = str(int(time.time()))
            data_list.append((insert_timestamp, is_play, album_id, grade, is_exclusive, collects, logo, category,
                              song_count, sub_title,
                              artist_id, status, recommends, description, check_rate, is_check, play_count,
                              gmt_publish, cd_count, artist_name, company, language, album_name))
        return data_list

    def get_single_page_albums(self, latest_time_stamp=0, page_index=1):
        '''
        :param latest_time_stamp: the latest time of last time
        :param page_index: page index
        :return:
        '''
        response_huayu = get_promotion_albums(type='huayu', limit=15, page=page_index)
        rank_promotio_albums_huayu = response_huayu['alibaba_xiami_api_rank_promotion_albums_get_response']['datas'][
            'rank_promotio_albums']

        response_oumei = get_promotion_albums(type='oumei', limit=15, page=page_index)
        rank_promotio_albums_oumei = response_oumei['alibaba_xiami_api_rank_promotion_albums_get_response']['datas'][
            'rank_promotio_albums']

        response_ri = get_promotion_albums(type='ri', limit=15, page=page_index)
        rank_promotio_albums_ri = response_ri['alibaba_xiami_api_rank_promotion_albums_get_response']['datas'][
            'rank_promotio_albums']

        response_han = get_promotion_albums(type='han', limit=15, page=page_index)
        rank_promotio_albums_han = response_han['alibaba_xiami_api_rank_promotion_albums_get_response']['datas'][
            'rank_promotio_albums']
        single_albums = rank_promotio_albums_huayu + rank_promotio_albums_oumei + rank_promotio_albums_ri + rank_promotio_albums_han
        temp = []
        for album in single_albums:
            if album['gmt_publish'] > latest_time_stamp:
                temp.append(album)
        if len(temp) > 0:
            single_albums = temp
            return single_albums
        else:
            return []

    def interface_xiami_albums(self):
        # all_list中记录所有10页的专辑
        all_list = []
        latest_time = self._get_latest_time()
        for page_index in range(1, self.LIMIT_PAGE + 1):
            all_list += self.get_single_page_albums(latest_time_stamp=latest_time, page_index=page_index)
        for album in all_list:
            self.time_stamp.append(album['gmt_publish'])
        to_be_inserted = self.get_first(all_list)
        self.insert_into_mysql(data_list=to_be_inserted)
        # 记录本次插入最新专辑的时间戳
        latest_time = self.get_max_time(self.time_stamp)
        self._set_latest_time(latest_time)

    def interface_continue_work(self):
        all_list = []
        latest_time = self._get_latest_time()
        for page_index in range(1, self.LIMIT_PAGE + 1):
            all_list += self.get_single_page_albums(latest_time_stamp=latest_time,
                                                    page_index=page_index)
        del self.time_stamp[:]
        if len(all_list) > 0:
            length = len(all_list)
            fn = open('work_log.log', 'a')
            fn.write(str(time.time()) + '\t' + 'insert ' + str(length))
            fn.write('\n')
            fn.flush()
            fn.close()
            for album in all_list:
                self.time_stamp.append(album['gmt_publish'])
            to_be_inserted = self.get_new(all_list)
            self.insert_into_mysql(data_list=to_be_inserted)
            latest_time = self.get_max_time(self.time_stamp)
            self._set_latest_time(latest_time)
        else:
            fn = open('work_log.log', 'a')
            fn.write(str(time.time()) + '\t' + 'insert ' + '0')
            fn.write('\n')
            fn.flush()
            fn.close()

    def insert_into_mysql(self, data_list):
        '''
        :param data_list: the list contains data to be inserted into mysql
        :return: None
        '''
        sql = 'insert into xiami_albums' \
              '(insert_timestamp,is_play, album_id, grade, is_exclusive, collects, logo, category, ' \
              'song_count,sub_title,artist_id, status, recommends, description, check_rate, is_check, ' \
              'play_count,gmt_publish,cd_count, artist_name, company, my_language, album_name)values ' \
              '(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        try:
            self.cursor.executemany(sql, data_list)
            self.database.commit()
            self.cursor.close()
            self.database.close()
        except Exception, e:
            fn = open('error.log', 'a')
            fn.write(str(int(time.time())) + '  ' + str(e))
            fn.write('\n')
            fn.flush()
            fn.close()
            self.database.rollback()
            self.cursor.close()
            self.database.close()

    def get_max_time(self, lists):
        '''
        :param lists:list which contains many time stamps
        :return: the max time stamp
        '''
        max_time = 0
        for time in lists:
            if time > max_time:
                max_time = time
            else:
                pass
        return max_time

    def _get_latest_time(self):
        '''
        :return:the time stamp of inserting data into database last time
        '''
        fn = open('time_stamp.txt', 'r')
        latest_time = int(fn.readline().strip())
        fn.close()
        return latest_time

    def _set_latest_time(self, time_stamp):
        '''
        :param time_stamp:the latest time stamp
        :return:None
        '''
        fn = open('time_stamp.txt', 'w')
        fn.write(str(time_stamp))
        fn.write('\n')
        fn.flush()
        fn.close()
        return


if __name__ == '__main__':
    while True:
        obj = WorkWork(limit=10)
        obj.interface_continue_work()
        time.sleep(86400)
