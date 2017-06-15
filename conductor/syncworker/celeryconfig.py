"""
Celery config
"""


class CeleryConfig(object):
    broker_url = 'amqp://syncman:SyncMan@146.148.75.194/syncman'
    result_backend = 'rpc://'
    task_default_queue = 'celery'
    task_default_routing_key = 'celery'
    worker_prefetch_multiplier = 1
    task_acks_late = True
    task_reject_on_worker_lost = True
    task_track_started = True
    worker_concurrency = 1
    worker_send_task_events = True
    task_send_sent_event = True
