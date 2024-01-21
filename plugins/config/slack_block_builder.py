def divider():
    return  {
        "type": "divider"
    }


def mrkdwn_text(message):
    return  {
        "type": "mrkdwn",
        "text": f"{message}"
    }
    

def section_text(message):
    return  {
        "type": "section",
        "text": mrkdwn_text(message)
    }
