import smtplib

from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText


def send_email(msg_body, subject, toaddr="analysts@roundaboutam.com"):
    """
    Send an email with msg in the body from the notifications@roundaboutam.com
    address.
    """
    fromaddr = "notifications@roundaboutam.com"
    passwd = "ramalerts"

    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = subject

    body = msg_body
    msg.attach(MIMEText(body, 'plain'))
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(fromaddr, passwd)
    text = msg.as_string()
    server.sendmail(fromaddr, toaddr, text)
    server.quit()
    return
