import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import pathlib

SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')
ADMIN_EMAIL_ADDRESS = os.getenv('ADMIN_EMAIL_ADDRESS')

class NotificationEntity:
    def __init__(self, id, status, message, subject, completedDate):
        self.id = id
        self.message = message
        self.subject = subject
        self.completedDate = completedDate

    def setNotificationCompleted(self, totalAttendees):
        self.completedDate = datetime.utcnow()
        self.status = 'Notified {} attendees'.format(totalAttendees)


class AttendeeEntity:
    def __init__(self, firstName, lastName, email):
        self.firstName = firstName
        self.lastName = lastName
        self.email = email


class AttendeeRepo:
    def __init__(self):
        self.queryAllAttendeesEmail = "SELECT first_name,last_name,email FROM attendee;"

    def getAttendees(self, cursor):
        qNbyId = self.queryAllAttendeesEmail.format(id)
        cursor.execute(qNbyId)
        nrows = cursor.fetchall()
        results = []
        for row in nrows:
            results.append(
                AttendeeEntity(
                    firstName=row[0],
                    lastName=row[1],
                    email=row[2]
                )
            )
        return results


class NoticationRepo:

    def __init__(self):
        self.queryById = "SELECT id,status,message,subject,completed_date FROM notification where id={};"
        self.queryUpdateToCompleted = "UPDATE notification SET status= %s, completed_date = %s WHERE id = %s"

    def getById(self, id, cursor):
        q = self.queryById.format(id)
        cursor.execute(q)
        nrows = cursor.fetchone()
        if nrows != None:
            return NotificationEntity(
                id=nrows[0],
                status=nrows[1],
                message=nrows[2],
                subject=nrows[3],
                completedDate=nrows[4]
            )
        return None

    def setCompleted(self, n: NotificationEntity, cursor, conn):
        try:
            cursor.execute(self.queryUpdateToCompleted,
                           (n.status, n.completedDate, n.id))

            logging.info("Updated Rows {}".format(cursor.rowcount))

            conn.commit()
            return True
        except Exception as e:
            logging.error('Could commit changes with error: ' + str(e))
            return False


def get_ssl_cert():
    current_path = pathlib.Path(__file__).parent
    logging.info(current_path)
    return str(current_path / 'BaltimoreCyberTrustRoot.crt.pem')


def get_conn(sslpath: str):
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASS'),
        sslmode='require',
        sslrootcert=sslpath,
        database=os.getenv('DB_NAME')
    )


def closeDb(cursor, connection):
    try:
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(
            'Could not close connection and cursor with error: ' + str(e))


def send_email(email, subject, message):
    logging.info('SENDGRID KEY: {} - Sending email to {} with subject {} and message {}'.format(
        SENDGRID_API_KEY, email, subject, message))

    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(
            Mail(
                from_email=ADMIN_EMAIL_ADDRESS,
                to_emails=email,
                subject=subject,
                html_content='<strong>{}</strong>'.format(message)
            )
        )
        if response.status_code > 200:
            return True
    except Exception as e:
        logging.error('SEND_MAIL Exception: {}'.format(str(e)))
    return False


SSL_PATH = get_ssl_cert()


def main(message: func.ServiceBusMessage):
    notification_id = None
    try:
        notification_id = int(message.get_body().decode('utf-8'))
        logging.info(
            'Python ServiceBus queue trigger processed message: %s', notification_id)
    except Exception as e:
        logging.error('Invalid entry, skipping message {}'.format(message.get_body().decode('utf-8')))
        return

    conn = None
    cur = None
    nRepo = NoticationRepo()

    try:
        conn = get_conn(SSL_PATH)
        logging.info("Connection successful")

        cur = conn.cursor()
        n = nRepo.getById(notification_id, cur)
        if n != None:
            ats = AttendeeRepo().getAttendees(cur)
            for at in ats:
                subject = '{}: {}'.format(at.firstName, n.subject)
                send_email(at.email, n.subject, n.message)

            n.setNotificationCompleted(len(ats))
            nRepo.setCompleted(n, cur, conn)

    except Exception as e:
        logging.error('General Exception: ' + str(e))
    finally:
        closeDb(cur, conn)