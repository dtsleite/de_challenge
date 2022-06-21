import yagmail
import config as p


def send_email_alert(bd,sj):
    receiver = p.get('E_MAIL_RECEIVER')
    body = bd

    yag = yagmail.SMTP("dtsleite@gmail.com")
    
    '''
    yag.send(
        to=p.get('E_MAIL_RECEIVER'),
        subject=sj,
        contents=bd, 
    )
    '''
