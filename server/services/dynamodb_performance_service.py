from datetime import datetime


class PerformanceData:

    def __init__(self, data, from_datetime, to_datetime):
        self.data = data
        self.from_datetime = from_datetime
        self.to_datetime = to_datetime


class DynamodbPerformanceService:

    def __init__(self, context):
        self.context = context
        self.table_index_performance_dict = {}
        self.performance_start_datetime = datetime.utcnow()

    def update_counter(self, table_name, operation_type, is_error=False, index=None):
        key = f"{table_name.split('_')[-1]}:{operation_type}:{is_error}"
        if index:
            key = f"{key}:{index}"

        if key not in self.table_index_performance_dict:
            self.table_index_performance_dict[key] = 0
        self.table_index_performance_dict[key] += 1

    def get_performance_data(self):
        now = datetime.utcnow()
        data_copy = self.table_index_performance_dict.copy()
        result = PerformanceData(data_copy, self.performance_start_datetime, now)

        self.table_index_performance_dict = {}
        self.performance_start_datetime = now
        return result
