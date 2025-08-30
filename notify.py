import os
import json
import urllib.request
import smtplib
import ssl
from email.message import EmailMessage
import pandas as pd

def build_summary():
    path = os.path.join('data', 'new_rows.csv')
    if os.path.exists(path):
        df = pd.read_csv(path)
        count = len(df)
        return count, (f"VC sourcing run added {count} new leads" if count else "No new leads added")
    return 0, 'No new rows file found'

def post_slack(webhook_url, message):
    data = json.dumps({'text': message}).encode('utf-8')
    req = urllib.request.Request(webhook_url, data=data, headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req) as resp:
        resp.read()

def send_email(host, port, username, password, to_addr, from_addr, message):
    msg = EmailMessage()
    msg['Subject'] = 'VC Sourcing Summary'
    msg['From'] = from_addr
    msg['To'] = to_addr
    msg.set_content(message)
    context = ssl.create_default_context()
    with smtplib.SMTP(host, int(port)) as server:
        server.starttls(context=context)
        server.login(username, password)
        server.send_message(msg)

def main():
    _, summary = build_summary()
    print(summary)

    slack_url = os.getenv('SLACK_WEBHOOK_URL')
    if slack_url:
        try:
            post_slack(slack_url, summary)
            print('Sent Slack notification')
        except Exception as e:
            print(f'Slack notification failed: {e}')

    smtp_vars = {k: os.getenv(k) for k in ['SMTP_HOST', 'SMTP_PORT', 'SMTP_USERNAME', 'SMTP_PASSWORD', 'SMTP_TO', 'SMTP_FROM']}
    if all(smtp_vars.values()):
        try:
            send_email(
                smtp_vars['SMTP_HOST'], smtp_vars['SMTP_PORT'],
                smtp_vars['SMTP_USERNAME'], smtp_vars['SMTP_PASSWORD'],
                smtp_vars['SMTP_TO'], smtp_vars['SMTP_FROM'], summary
            )
            print('Sent email notification')
        except Exception as e:
            print(f'Email notification failed: {e}')

if __name__ == '__main__':
    main()
