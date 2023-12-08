from dagster import DynamicPartitionsDefinition, RunRequest, SensorResult, sensor

from ..resources import GirderConnection


def make_girder_folder_sensor(
    job, folder_id, sensor_name, partitions_def: DynamicPartitionsDefinition
):
    @sensor(name=sensor_name, job=job)
    def folder_contents(context, girder: GirderConnection):
        new_items = []
        context.log.info(f"Checking for new items in folder {folder_id}")
        for item in girder.list_item(folder_id):
            context.log.info(f"Checking item {item['name']}")
            if not context.instance.has_dynamic_partition(
                partitions_def.name, item["name"]
            ):
                new_items.append(item)
        run_requests = []
        for item in new_items:
            tags = partitions_def.get_tags_for_partition_key(item["name"])
            tags["itemId"] = item["_id"]
            run_requests.append(
                RunRequest(
                    partition_key=item["name"],
                    tags=tags,
                )
            )
        return SensorResult(
            run_requests=run_requests,
            dynamic_partitions_requests=[
                partitions_def.build_add_request([_["name"] for _ in new_items])
            ],
        )

    return folder_contents
