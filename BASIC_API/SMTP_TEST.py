import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart



# Office 365 SMTP server settings
SMTP_SERVER = "smtp.office365.com"
SMTP_PORT = 587  # TLS port
SMTP_USERNAME = "Tecno.Tree@ss.zain.com"  # Your Office 365 email address
SMTP_PASSWORD = "Zain@2022"  # Your Office 365 password
#SMTP_USERNAME = "jeethraj.poojary@tecnotree.com"  # Your Office 365 email address
#SMTP_PASSWORD = "Mang@0101"  # Your Office 365 password

# Email content
TO_EMAIL = "jeethraj.poojary@tecnotree.com"  # Recipient's email address
#SENDER_EMAIL = "DailyReports@ss.zain.com"
SUBJECT = "Subject of the email"
MESSAGE = "Hello, this is a test email."

# Create an email message
msg = MIMEMultipart()
msg['From'] = SMTP_USERNAME
msg['To'] = TO_EMAIL
msg['Subject'] = SUBJECT
msg.attach(MIMEText(MESSAGE, 'plain'))

# Connect to the Office 365 SMTP server and send the email with SSL handshake debugging
try:
    server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
    #server.set_debuglevel(1)  # Set debug level to 1 to enable debugging
    server.starttls()
    server.login(SMTP_USERNAME, SMTP_PASSWORD)
    server.sendmail(SMTP_USERNAME, TO_EMAIL, msg.as_string())
    server.quit()
    print("Email sent successfully.")
except Exception as e:
    print(f"Error sending email: {str(e)}")
