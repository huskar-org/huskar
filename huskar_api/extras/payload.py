from huskar_api.extras.monitor import monitor_client


def zk_payload(payload_data, payload_type):
    monitor_client.payload("zookeeper.payload",
                           tags=dict(payload_type=payload_type),
                           data_length=len(
                            payload_data) if payload_data else 0)
