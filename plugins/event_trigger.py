# airflow의 과정 중, 이벤트를 발생시키는 동작 수행
# 1. slack을 통해 알람을 보내고 싶을 때, 활용하는 slack_bot

from airflow.models import Variable
from airflow.operators.slack_operator import SlackAPIOperator

class SlackAlert:
    def __init__(self, channel):
        self.slack_channel = channel
        self.token = Variable.get("slack_bot_token")

    def slack_fail_alert(self, context):
        alarm_task = SlackAPIOperator(
            task_id="slack_fail",
            channel=self.slack_channel,
            token=self.token,
            text=f"""
                 :red_circle: Task Failed.
                *Task*: {context.get('task_instance').task_id}  
                *Dag*: {context.get('task_instance').dag_id}
                *Execution Time*: {context.get('execution_date')}  
                *Log Url*: {context.get('task_instance').log_url}
            """)

        return alarm_task.execute(context=context)