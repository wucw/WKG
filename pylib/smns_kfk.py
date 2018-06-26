#!_NS_INSX/python/bin/python
# -*- coding: utf-8 -*-

# by mazcheng on 2017-04-07
# by mazcheng @20180323
# offer a new api for kafka, suggest api 2

"""
api 1[DISCARDED]::use some function api, only support producer
    A demo of smns_kfk: Get some msg_lists(element is msg_list[dict(as a msg), ]) --> Kafka
    producer must be started when start producing, i.e. must be stopped when stop producing
	```
	import smns_kfk
	client = smns_kfk.conn2kfkclient(logger, hosts)
	producer = smns_kfk.client2producer(logger, client, topicname, max_request_size=1000012, linger_ms=500)
	for msg_list in msg_lists:
		producer = smns_kfk.list2kfk(logger, producer, msg_list, ctrl_num=1000, auto_reconn=False, reconn_timeout=10, stop_lock=[])
	smns_kfk.stop_produce(logger, producer)
api 2::use a KfkTopicService class api, package from usage 1, optimize the code, support prodcer & consumer, reduce the coupling
    in sum, the usage is like usage 1
    ```
    kfk_topic = smns_kfk.KfkTopicService(logger, stop_lock, kfk_conf_file, SELF)
    topicname = 'fw_module_scan'
    tp = kfk_topic.get_topic(topicname)
    # produce
    msg_list = []
    with open('/apps/ns/platform/pylib/dong_guan/fw_module_scan.json', 'r') as f:
        for line in f:
            msg_list.append(line.strip())
    producer = kfk_topic.get_producer(tp)
    kfk_topic.list2kfk(producer, msg_list)
    kfk_topic.stop_producer(producer)
    # consume
    consumer = kfk_topic.get_consumer(tp)
    msg_list = kfk_topic.kfk2list(consumer)
    kfk_topic.stop_consumer(consumer)
    for msg in msg_list:
        print msg
	```
"""

import os
import sys
import json
import time
import Queue
import logging
import ConfigParser
from functools import wraps
from pykafka import KafkaClient
from pykafka.simpleconsumer import OffsetType
from pykafka import exceptions

sys.path.append('/apps/ns/platform/../pylib')
internal_logger_dirname_path = '/apps/ns/platform/../log'

__module_name__ = 'smns_kfk'


def get_kafka_args(logger, kfk_conf_file, pname):
    global external_logger_name  # for _internal_logger
    global en_internal_logger  # for conn2kfkclient
    external_logger_name = pname
    cf = ConfigParser.ConfigParser()
    cf.read(kfk_conf_file)
    try:
        hosts = cf.get(pname, 'hosts')
        en_internal_logger = cf.getboolean(pname, 'en_internal_logger')
    except Exception, e:
        logger.error("cfg [%s] get FAIL [%s]", kfk_conf_file, str(e))
        return None
    logger.info("%s.%s [%s]", pname, 'db_args', hosts)
    return hosts


def conn2kfkclient(logger, hosts, broker_version='0.8.2'):
    """Connect to KafkaClient and Get a Producer
    :param logger(ns_log.logger): Record log
    :param hosts(str): Comma-separated list of kafka hosts to which to connect
    :return client(pykafka.KafkaClient): Connected client
    """
    try:
        if en_internal_logger:
            _internal_logger()  # fix No handlers could be found for logger "pykafka.cluster" WARNING
        else:
            logger.debug('You don\'t open internal logger for Pykafka')
    except NameError:
        _internal_logger()

    global ho  # for _reconnect function
    ho = hosts
    try:
        client = KafkaClient(hosts=hosts, broker_version=broker_version)
        return client
    except exceptions.NoBrokersAvailableError as e:
        logger.error('NoBrokersAvailableError, KafkaClient connect fail [%s][%s]', hosts, str(e))
        return None
    except Exception, e:
        logger.error('kafka client conn fail [%s][%s]', hosts, str(e))
        return None


def client2producer(logger, client, topicname, max_request_size=1000012, linger_ms=5000, ):
    """
    Get a Producer from a topic in client
    :param topicname(str): The topicname as key to find which to produce messages
    :return producer(pykafka.producer): Started Producer
    """
    global tp  # for _reconnect function
    tp = topicname
    if client:
        try:
            topic = client.topics[topicname]
            if topic:
                logger.info('found this topic [%s]', topicname)
            else:
                logger.warn('will create the new topic [%s]', topicname)
                time.sleep(2)  # wait for kafka reconn
                topic = client.topics[topicname]
            producer = topic.get_producer(delivery_reports=True, auto_start=False, \
                                          max_request_size=max_request_size,
                                          linger_ms=linger_ms)  # linger_ms can smaller
            producer.start()
            return producer
        except Exception, e:
            logger.error('client2producer meet Exception [%s][%s]', topicname, str(e))
            return None
    else:
        return None


# by mazcheng @20180317
def client2consumer(logger, client, topicname, **kwargs):
    """
    GET a Comsumer from a topic in client
    :param topicname(str): The topicname as key to find which to produce messages
    :param kwargs: simpleconsumer.kwargs
    :return: consumer(pykafka.simpleconsumer)
    get_simple_consumer consumer_group=None, use_rdkafka=False
    """
    global tp
    tp = topicname
    if client:
        try:
            topic = client.topics[topicname]
            if topic:
                logger.info('found this topic [%s]', topicname)
            else:
                logger.warn('will create the new topic [%s]', topicname)
                time.sleep(2)  # wait for kafka reconn
                topic = client.topics[topicname]
            # consumer = topic.get_simple_consumer(use_rdkafka=False, consumer_timeout_ms=100,
            #                                      auto_offset_reset=OffsetType.LATEST, reset_offset_on_start=False)
            consumer = topic.get_simple_consumer(auto_commit_enable=True, auto_commit_interval_ms=1000,
                                                 auto_offset_reset=OffsetType.LATEST, reset_offset_on_start=True)
            # consumer_timeout_ms = 100
            # auto_offset_reset=OffsetType.LATEST
            # , reset_offset_on_start=True
            # consumer = topic.get_balanced_consumer(consumer_group=b"group2",zookeeper_connect="172.30.154.62:2181",consumer_timeout_ms=100,auto_commit_enable=True)
            # , reset_offset_on_start = True, auto_offset_reset = OffsetType.LATEST
            consumer.start()
            print consumer.held_offsets
            # consumer.consume()
            # consumer.commit_offsets()
            return consumer
        except Exception as e:
            logger.error('client2consumer meet Exception [%s][%s]', topicname, str(e))
            return None
    else:
        return None


def client2consumer2(logger, client, topicname, **kwargs):
    """
    GET a Comsumer from a topic in client
    :param topicname(str): The topicname as key to find which to produce messages
    :param kwargs: simpleconsumer.kwargs
    :return: consumer(pykafka.simpleconsumer)
    get_simple_consumer consumer_group=None, use_rdkafka=False
    """
    global tp
    tp = topicname
    if client:
        try:
            topic = client.topics[topicname]
            if topic:
                logger.info('found this topic [%s]', topicname)
            else:
                logger.warn('will create the new topic [%s]', topicname)
                time.sleep(2)  # wait for kafka reconn
                topic = client.topics[topicname]
            # consumer = topic.get_simple_consumer(use_rdkafka=False, consumer_timeout_ms=100,
            #                                      auto_offset_reset=OffsetType.LATEST, reset_offset_on_start=False)
            consumer = topic.get_simple_consumer(consumer_group='g2', reset_offset_on_start=False,
                                                 auto_commit_enable=True, auto_commit_interval_ms=1000)
            # consumer_timeout_ms = 100
            # auto_offset_reset=OffsetType.LATEST
            # , reset_offset_on_start=True
            # consumer = topic.get_balanced_consumer(consumer_group=b"g2",zookeeper_connect="172.30.154.62:2181",consumer_timeout_ms=100)
            # , reset_offset_on_start = True, auto_offset_reset = OffsetType.LATEST
            consumer.start()
            print consumer.held_offsets
            # consumer.consume()
            # consumer.commit_offsets()
            return consumer
        except Exception as e:
            logger.error('client2consumer meet Exception [%s][%s]', topicname, str(e))
            return None
    else:
        return None


def list2kfk(logger, producer, msg_lst, ctrl_num=1000, auto_reconn=False, reconn_timeout=10, stop_lock=[]):
    """Make message List Push to Broker
    :param logger(ns_log.logger): Record log
    :param producer(pykafka.producer): Connected client topic producer
    :param msg_lst(list): Will produce message list as .json send to broker
    :param ctrl_num(int): Adjust this or bring lots of RAM
    :param auto_reconn(bool): If auto_reconn is True, will use a infinite loop to reconnect. Default False
    :param reconn_timeout(int): keep the frequency to reconnect Kafka
    :return producer(pykafka.producer): As next message delivery or be closed by stop_produce
    """
    if not producer:
        producer = _reconnect(logger, ho, tp, producer, auto_reconn, reconn_timeout, stop_lock)
    st = time.time()
    delivery_num = 0
    try:
        for msg in msg_lst:
            #  type(msg) == <Dict> ensure this Dict's key and value is **unicode** instance
            delivery_num += 1
            en_msg = json.dumps(msg, ensure_ascii=False).encode('utf-8')  # encode as utf-8
            #  en_msg = bytes(msg)
            producer.produce(en_msg)
            if delivery_num % ctrl_num == 0:
                _is_finish(logger, producer)
        _is_finish(logger, producer)
        #  fix correct time & speed (when stop all queue readers needs more time)
        et = time.time()
        during = et - st
        speed = delivery_num / during
        logger.info('succeed in delivering message [%d] use [%fs], speed [%d/s]', delivery_num, float(during), speed)
        return producer
    except exceptions.SocketDisconnectedError as e:
        logger.error('SocketDisconnectedError, Disconnect KafKa [%s]', str(e))
        producer = _reconnect(logger, ho, tp, producer, auto_reconn, reconn_timeout, stop_lock)
        return producer
    except IOError as e:
        logger.error('IOError, Kafka not running [%s]', str(e))
        producer = _reconnect(logger, ho, tp, producer, auto_reconn, reconn_timeout, stop_lock)
        return producer
    except Exception, e:
        logger.error('Unknown Exception, Fail to deliver msg [%s]', str(e))
        return None


def kfk2list(logger, consumer):
    """
    :param logger:
    :param consumer:
    :return:
    """
    msg_list = []
    try:
        print consumer.held_offsets
        # print consumer.fetch_offsets()
        for msg in consumer:
            print msg.offset
            if msg.offset > 3010:
                break
    except Exception as e:
        logger.error('kfk2list consumer Err [%s]', e)
    print consumer.held_offsets
    consumer.commit_offsets()
    # print  consumer.fetch_offsets()


def stop_produce(logger, producer):
    st = time.time()
    if producer:
        try:
            producer.stop()
            et = time.time()
            during = et - st
            logger.info('succeed in stopping producer [%s] use [%fs]', producer._topic.name, float(during))
        except exceptions.ProducerStoppedException as e:
            logger.error('ProducerStoppedException, current producer has been stopped already [%s]', str(e))
        except IOError as e:
            logger.error('IOError, producer stop error [%s]', str(e))
        except Exception as e:
            logger.error('Unknown Exception, Fail to stop producer [%s]', str(e))


def _is_finish(logger, producer):
    while 1:
        try:
            sent_msg, exc = producer.get_delivery_report(block=False)
            if exc:
                logger.error('Fail to deliver msg: {}'.format(repr(exc)))
                # else:
                #	logger.info('succeed in delivering msg')
        except Queue.Empty:
            break


def _reconnect(logger, hosts, topicname, producer, auto_reconn, reconn_timeout, stop_lock=[]):
    """Support Re connect Policy
    :param hosts(str from conn2kfkclient):
    :param topicname(str from client2producer):
    :param auto_reconn(bool from list2kfk):
    :param reconn_timeout(int from list2kfk):
    """
    reconn_num = 0
    stop_produce(logger, producer)
    while auto_reconn:
        if stop_lock:
            if stop_lock[0]:
                break
        reconn_num += 1
        logger.info('Reconnecting the [%d]th', reconn_num)
        time.sleep(reconn_timeout)
        cli = conn2kfkclient(logger, hosts)
        producer = client2producer(logger, cli, topicname)
        if producer:
            break
    return producer


def _internal_logger():
    """
    fix by mazcheng on 2017-04-20
    make a internal and maintainable logger only for PyKafka>smns_kfk
    2 namings:
        smns_kfk_%H%M%S.log
        smns_kfk_external_program.log
    """
    current = time.strftime("%Y%m%d", time.localtime(time.time()))
    try:
        smns_kfk_logger_name = 'smns_kfk_' + external_logger_name + '.log'
    except NameError:
        now = time.strftime('%H%M%S', time.localtime(time.time()))
        smns_kfk_logger_name = 'smns_kfk_' + now + '.log'
    smns_kfk_logger_path = os.path.join(log_path, current, smns_kfk_logger_name)
    smns_kfk_fmt = '%(asctime)s <%(levelname)-5s><%(name)s:%(process)d:%(threadName)s><%(filename)s:%(lineno)d>%(message)s'
    logging.basicConfig(filename=smns_kfk_logger_path, format=smns_kfk_fmt, level=logging.NOTSET)


class KfkTopicService(object):
    """
    Encapsulate Pykafk in common & Use Kfk in easily
    one kfkclient -> more kfktopic --> more producer, more consumer
    Not support Serializer & Deserializer [make message cover to valid type outside]
    """

    def __init__(self, logger, stop_lock, kfk_conf_file, pname=None):
        """
        Init KafkaService object with configure file
        :param logger: Logger Object
        :param kfk_conf_file: str::kafka configure file path
        :param pname: str::who use this Class KafkaService [for pykafk internal logger & consumer group name]
        """
        self.logger = logger
        self.stop_lock = stop_lock if isinstance(stop_lock, list) else [False, ]
        self.project_name = pname if pname else time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
        self.config = self.__init_conf(kfk_conf_file)
        self.client = self._init_client()

    def __init_conf(self, kfk_conf_file, extra_config={}):
        """
        Init Configure Main func
        :param kfk_conf_file: str::kafka configure file path
        :param extra_config: dict::extra configure
        :return: dict::config
        """
        config = {}
        cf = ConfigParser.ConfigParser()
        str_opts = (
            'hosts',
            'internal_logger_lvl',
        )
        bool_opts = (
            'kfk_reconn',
        )
        int_opts = (
            'ctrl_num',
            'kfk_reconn_timeout',
            'max_msg_num',
            'consume_mode',
        )
        try:
            cf.read(kfk_conf_file)
            for opt in str_opts:
                config[opt] = cf.get(self.project_name, opt)
            for opt in bool_opts:
                config[opt] = cf.getboolean(self.project_name, opt)
            for opt in int_opts:
                config[opt] = cf.getint(self.project_name, opt)
            config.update(extra_config)
            for opt in config:
                self.logger.debug('kfk config %s.%s [%s]', self.project_name, opt, config[opt])
        except Exception as e:
            self.logger.error('kfk config file [%s] Err [%s]', kfk_conf_file, e)
        return config

    def _init_client(self, **kwargs):
        """
        Connect to KafkaClient
        @refer: http://pykafka.readthedocs.io/en/latest/api/client.html
        :param kwargs: dict::keyword argument for API `KafkaClient`'s kwargs (else `broker_version`)
        :return: client(pykafka.KafkaClient): Connected client
        """
        self.__open_internal_logger()
        client = None
        hosts = self.config.get('hosts')
        """
        >>> specify arguments by default <<<
        broker_version::0.8.2::current internal business only support kafka 0.8.2
        """
        client_kwargs = dict(hosts=hosts, broker_version='0.8.2')
        client_kwargs.update(kwargs)
        if hosts:
            while self.config.get('kfk_reconn'):
                try:
                    client = KafkaClient(**client_kwargs)
                    self.logger.info('conn2kfkclient hosts [%s] ok', hosts)
                except exceptions.NoBrokersAvailableError as e:
                    self.logger.error('conn2kfkclient hosts [%s] NoBrokersAvailableError Err [%s]', hosts, e)
                except Exception as e:
                    self.logger.error('conn2kfkclient hosts [%s] Err [%s]', hosts, e)
                if client:
                    break
                if self.stop_lock[0]:
                    break
                self.logger.error('reconnecting kfkclient and will sleep [%s]', self.config.get('kfk_reconn_timeout'))
                time.sleep(self.config.get('kfk_reconn_timeout'))
            else:
                try:
                    client = KafkaClient(**client_kwargs)
                    self.logger.info('conn2kfkclient hosts [%s] ok', hosts)
                except exceptions.NoBrokersAvailableError as e:
                    self.logger.error('conn2kfkclient hosts [%s] NoBrokersAvailableError Err [%s]', hosts, e)
                except Exception as e:
                    self.logger.error('conn2kfkclient hosts [%s] Err [%s]', hosts, e)
        else:
            self.logger.error('conn2kfkclient Err [%s]', 'hosts unavailable')

        return client

    def get_topic(self, topicname):
        """
        Get a Topic from Client
        @refer: http://pykafka.readthedocs.io/en/latest/api/topic.html
        :param topicname: str::Kafka's topic
        :return: topic::Kafka's Topic
        """
        topic = None
        if self.client:
            # topic = self.client.topics.get(topicname)
            topic = self.client.topics[topicname]
            if topic:
                self.logger.info('get topic found this topic [%s]', topicname)
            else:
                self.logger.debug('recreate dummy topic [%s]', topicname)
                time.sleep(2)
                topic = self.client.topics[topicname]
        else:
            self.logger.error('get topic Err [%s]', 'client unavailable')

        return topic

    def get_producer(self, topic, **kwargs):
        """
        Get a Producer from Topic
        @refer: http://pykafka.readthedocs.io/en/latest/api/producer.html
        :param topic: Pykafka Topic object
        :param kwargs: keyword argument for API `producer`'s kwargs
        :return: producer::Kafka's Producer started
        """
        producer = None
        """
        >>> specify arguments by default <<<
        delivery_reports::True::open pykafk internal report to record produced message
        """
        producer_kwargs = dict(delivery_reports=True)
        producer_kwargs.update(kwargs)
        if topic:
            producer = topic.get_producer(**producer_kwargs)
        else:
            self.logger.error('get producer Err [%s]', 'topic unavailable')

        return producer

    def get_consumer(self, topic, **kwargs):
        """
        Get a Consumer from Topic
        @refer: http://pykafka.readthedocs.io/en/latest/api/simpleconsumer.html
        :param topic: Pykafka Topic object
        :param kwargs: keyword argument for API `simpleconsumer`'s kwargs
        :return: simpleconsumer::Kafka's SimpleConsumer started
        """
        consumer = None
        """
        >>> specify arguments by default <<<
        BY `consume_mode` configure::control consume methods
        earliest:-2, lastest:-1, by_consumer_group:0 @ref: Pykafka.OffsetType

        earliest:-2::consume the earliest message if control specify offset by external configure
            auto_commit_enable::True::auto commit offsets to kafka
            auto_commit_interval_ms::10000::interval 10s if auto_commit_enable [default 1min]
            reset_offset_on_start::True::autoly open reset offset when start
            auto_offset_reset::OffsetType.EARLIEST::only start consuming from the earliest message in kafka
            consumer_timeout_ms::60000::consume timeout

        lastest:-1::consume the lastest message if consume timeout: stop consume
            auto_commit_enable::True::auto commit offsets to kafka
            auto_commit_interval_ms::10000::interval 10s if auto_commit_enable [default 1min]
            reset_offset_on_start::True::autoly open reset offset when start
            auto_offset_reset::OffsetType.LATEST::only start consuming from the lastest message in kafka
            consumer_timeout_ms::60000::consume timeout

        by_consumer_group:0::[default]consume the message from history offset which remembered by kafka itself by consumer group name
            consumer_group::self.project_name::specify consumer group name to keep its offset
            auto_start::False::reset not-auto-start produce when inited
            consumer_timeout_ms::60000::consume timeout
        """
        if self.config['consume_mode'] == -2:
            # earliest
            consumer_kwargs = dict(
                consumer_group=self.project_name,
                reset_offset_on_start=True,
                consumer_timeout_ms=60000,
                auto_commit_enable=True,
                auto_offset_reset=OffsetType.EARLIEST,
                auto_commit_interval_ms=10000,
            )
        elif self.config['consume_mode'] == -1:
            # lastest
            consumer_kwargs = dict(
                consumer_group=self.project_name,
                reset_offset_on_start=True,
                consumer_timeout_ms=60000,
                auto_commit_enable=True,
                auto_offset_reset=OffsetType.LATEST,
                auto_commit_interval_ms=10000,
            )
        else:
            # by_consumer_group
            consumer_kwargs = dict(
                consumer_group=self.project_name,
                reset_offset_on_start=False,
                consumer_timeout_ms=60000
            )
        self.logger.debug('init consume mode [%s]',
                          {-2: 'EARLIEST', -1: 'LASTEST', 0: 'BY CONSUMER GROUP'}.get(self.config['consume_mode']))
        consumer_kwargs.update(kwargs)
        if topic:
            consumer = topic.get_simple_consumer(**consumer_kwargs)
        else:
            self.logger.error('get consumer Err [%s]', 'topic unavailable')

        return consumer

    def _produce_message(self, producer, msg):
        """
        only support single message produce
        :param producer: producer object started
        :param msg: str::queue_message
        """
        producer.produce(msg)

    def _consume_message(self, consumer):
        """
        only support single message consume
        :param consumer: consumer object started
        :return: msg str::queque_message
        """
        message = consumer.consume()
        if message:
            return message.value
        else:
            return None

    def list2kfk(self, producer, msg_list):
        """
        push mass message list to kafka

        require: type(msg) is in (str, bytes, unicode)
        if want json serialize to str pls use:
            json.dumps(msg, ensure_ascii=False).encode('utf8')
        NOT Serializer

        :param producer: Pykafka's producer object started
        :param msg_list: list/tuple element::str/dict/array
        :param is_ascii: whether open json.dumps:ensure_ascii [if T: default, unicode;else: can show Chinese]
        :return: producer::produced-producer
        """
        delivery_num = 0
        s_time = time.time()

        try:
            for msg in msg_list:
                delivery_num += 1
                self._produce_message(producer, msg)
                if delivery_num % self.config['ctrl_num'] == 0:
                    self.__finish_produce(producer)
                if self.stop_lock[0]:
                    break
            self.__finish_produce(producer)
            e_time = time.time()
            during = e_time - s_time
            speed = delivery_num / during
            self.logger.info('list2kfk success message [%d] use [%ss] speed [%d/s]', delivery_num, during, speed)
        except exceptions.SocketDisconnectedError as e:
            self.logger.error('list2kfk SocketDisconnectedError, Disconnect KafKa [%s]', e)
        except IOError as e:
            self.logger.error('list2kfk IOError, Kafka not running [%s]', e)
        except Exception as e:
            self.logger.error('list2kfk Unknown Exception, Fail to deliver msg [%s]', str(e))

        return producer

    def kfk2list(self, consumer, last_offset=0):
        """
        pull mass message from kafk to list

        NOT Deserializer

        offset control methods(with `get_consumer` init):
            config 1. unreliable method(drop some messages):
                discard history message, and always consume the lastest message
            config 2. foolish method(waste time):
                from the beginning to consume, let myself record the last consume offset, and next time filter by if statement
            config 3. clever method(strictly specify consumer group)[default]:
                let kafka itself record the last consume offset, and myself must specify the consumer group name

        :param consumer: Pykafka's consumer object started
        :param last_offset: int::last offset for config 2
        :return: consumer::consumed-consumer
        """
        delivery_num = 0
        s_time = time.time()
        msg_list = []

        if self.config['consume_mode'] == 0:
            # by_consumer_group
            try:
                for msg_obj in consumer:
                    delivery_num += 1
                    if delivery_num % self.config['ctrl_num'] == 0:
                        self.__finish_consume(consumer)
                    msg_list.append(msg_obj.value)
                    if delivery_num >= self.config['max_msg_num']:
                        break
                    if self.stop_lock[0]:
                        break
                self.__finish_consume(consumer)
                e_time = time.time()
                during = e_time - s_time
                speed = delivery_num / during
                self.logger.info('kfk2list success message [%d] use [%ss] speed [%d/s]', delivery_num, during, speed)
            except Exception as e:
                self.logger.error('kfk2list consumer Err [%s] if not get it, to debug internal_logger', e)
        else:
            # earliest/lastest
            try:
                for msg_obj in consumer:
                    delivery_num += 1
                    if delivery_num == 1:
                        print msg_obj.offset
                    msg_list.append(msg_obj.value)
                    if delivery_num >= self.config['max_msg_num']:
                        break
                    if self.stop_lock[0]:
                        break
                e_time = time.time()
                during = e_time - s_time
                speed = delivery_num / during
                self.logger.info('kfk2list success message [%d] use [%ss] speed [%d/s]', delivery_num, during, speed)
            except Exception as e:
                self.logger.error('kfk2list consumer Err [%s] if not get it, to debug internal_logger', e)

        return msg_list

    def __reconn2kfkclient(self, func):
        """
        Implement a Decorator for kafka reconnect to client operation
        :param func: callable method
        :return: reconn::callable
        """

        # TODO(mzc) pending, NOT Implement
        @wraps(func)
        def reconn(*args, **kwargs):
            # 判断 client 是否 已经连接 否则 重新连接
            # refer: https://www.cnblogs.com/tingfenglin/p/5592385.html
            # 带参数如: client,producer,consumer
            return func(*args, **kwargs)

        return reconn

    def __open_internal_logger(self):
        """
        fix by mazcheng on 2017-04-20
        make a internal and maintainable logger only for PyKafka>smns_kfk
        2 namings:
            `module_name`_%Y%m%d%H%M%S.log
            `module_name`_{{external_program}}.log
            level: CRITICAL,ERROR,WARNING,INFO,DEBUG,NOTSET
        """
        levels = {
            'CRITICAL': logging.CRITICAL,
            'ERROR': logging.ERROR,
            'WARNING': logging.WARNING,
            'INFO': logging.INFO,
            'DEBUG': logging.DEBUG,
            'NOTSET': logging.NOTSET,
        }
        level = levels.get(self.config['internal_logger_lvl'].upper())
        self.logger.debug('u not open internal logger for Pykafka which can debug Ur code better')
        current_date = time.strftime("%Y%m%d", time.localtime(time.time()))
        internal_logger_name = __module_name__ + '_' + self.project_name + '.log'
        internal_logger_path = os.path.join(internal_logger_dirname_path, current_date, internal_logger_name)
        internal_logger_fmt = '%(asctime)s <%(levelname)-5s><%(name)s:%(process)d:%(threadName)s><%(filename)s:%(lineno)d>%(message)s'
        logging.basicConfig(filename=internal_logger_path, format=internal_logger_fmt, level=level)

    def __is_produced_finish(self, producer):
        pass

    def __finish_produce(self, producer):
        while 1:
            try:
                sent_msg, exc = producer.get_delivery_report(block=False)
                if exc:
                    self.logger.error('finish produce Fail to deliver msg: [{}]'.format(repr(exc)))
                else:
                    self.logger.debug('finish produce succeed in delivering msg')
            except Queue.Empty:
                break

    def __finish_consume(self, consumer):
        try:
            consumer.commit_offsets()
            partition_offsets = consumer.held_offsets
            self.logger.debug('finish consume & partition offsets [%s]', str(partition_offsets))
        except Exception as e:
            self.logger.error('finish consume Err [%s]', e)

    def stop_producer(self, producer):
        st = time.time()
        if producer:
            try:
                producer.stop()
                et = time.time()
                during = et - st
                self.logger.info('stop producer topic [%s] use [%ss]', producer._topic.name, during)
            except exceptions.ProducerStoppedException as e:
                self.logger.error('stop producer ProducerStoppedException, current producer has been stopped [%s]', e)
            except IOError as e:
                self.logger.error('stop producer IOError, producer stop error [%s]', e)
            except Exception as e:
                self.logger.error('stop producer Unknown Exception, Fail to stop producer [%s]', e)

    def stop_consumer(self, consumer):
        st = time.time()
        if consumer:
            try:
                consumer.stop()
                et = time.time()
                during = et - st
                self.logger.info('stop consumer topic [%s] use [%ss]', consumer._topic.name, during)
            except Exception as e:
                self.logger.error('stop consumer Err [%s]', e)
