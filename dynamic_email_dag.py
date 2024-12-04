from airflow.decorators import dag, task
from airflow.providers.smtp.operators.smtp import EmailOperator
from pendulum import datetime

@dag(schedule_interval=None, start_date=datetime(2024, 12, 1), catchup=False,)
def dynamic_email_dag():
    @task
    def generate_email_content():
        return [
            {'recipient': 'c.mueller@ambifox.com', 'subject': 'Hello User1', 'body': 'Body for User1'},
            {'recipient': 'mail@kb135.de', 'subject': 'Hello User2', 'body': 'Body for User2'}
        ]

    @task
    def send_email(email_data):
        EmailOperator(
            task_id=f"send_email_{email_data['recipient'].replace('@', '_').replace('.', '_')}",
            to=email_data['recipient'],
            subject=email_data['subject'],
            html_content=email_data['body'],
            conn_id="aws-smtp"
        ).execute(context={})

    # Task-Aufruf
    emails = generate_email_content()
    send_email.expand(email_data=emails)  # expand() für parallele Ausführung nutzen

dynamic_email_dag = dynamic_email_dag()
