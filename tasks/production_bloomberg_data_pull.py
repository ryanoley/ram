import os
import time
import pandas as pd
import datetime as dt

import blpapi



class Blbg(object):

    def __init__(self, host='localhost', port=8194):
        """
        Starting bloomberg API session
        close with session.close()
        """
        # Fill SessionOptions
        sessionOptions = blpapi.SessionOptions()
        sessionOptions.setServerHost('localhost')
        sessionOptions.setServerPort(8194)

        # Create a Session
        self.session = blpapi.Session(sessionOptions)

        # Start a Session
        if self.session.start():
            print "Session started successfully"
        else:
            print "Failed to start session."

    def set_refData_service(self):
        """
        init service for refData
        """
        # Open service to get historical data from
        if not self.session.openService("//blp/refdata"):
            print "Failed to open //blp/refdata"

        # Obtain previously opened service
        self.refDataService = self.session.getService("//blp/refdata")

    def eqs(self, screen_name, screen_type="PRIVATE"):
        """
        Get either a PRIVATE or GLOBAL EQS screen and return the
        result as a DataFrame
        """

        request = self.refDataService.createRequest("BeqsRequest")
        request.set("screenName", screen_name)
        request.set("screenType", screen_type)

        print "Sending Request:", request

        # Send the request
        self.session.sendRequest(request)

        continueLoop = True

        while(continueLoop):
            # timeout to give the chance for Ctrl+C handling:
            ev = self.session.nextEvent(300)

            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completly received, so we could exit
                vals = self.handleEqsResponseEvent(ev)
                continueLoop = False
            else:
                self.handleOtherEvent(ev)

        data = pd.DataFrame(vals)
        return data

    def handleEqsResponseEvent(self, event):
        for msg in event:
            resp_data = msg.getElement('data')
            securityData = resp_data.getElement('securityData')
            d = []
            for i in range(securityData.numValues()):
                ind_data = securityData.getValue(i)
                field_data = ind_data.getElement('fieldData')
                tmp_dict = {}
                for j in range(field_data.numElements()):
                    fld_name = str(field_data.getElement(j).name())
                    element = field_data.getElementAsString(j)
                    tmp_dict[fld_name] = element
                d.append(tmp_dict)
        print 'END OF RESPONSE'
        return d

    def handleOtherEvent(self, event):
        for msg in event:
            print 'Msg: ' + str(msg.messageType())

    def stopSession(self):
        if self.session.stop():
            print "Session Stopped Successfully"


def write_output(data, file_name):
    file_name = dt.datetime.now().strftime('%Y%m%d') + \
        '_{}.csv'.format(file_name)
    file_path = os.path.join(os.getenv('DATA'), 'ram', 'implementation',
                             'bloomberg_data', file_name)
    data.to_csv(file_path, index=None)



def main():

    import os

    blbg = Blbg()
    blbg.set_refData_service()

    for i in range(10):
        try:
            data = blbg.eqs('QUANT_SCREENSPINOFF')
            write_output(data, 'spinoffs')
            break
        except:
            time.sleep(10)

    for i in range(10):
        try:
            data = blbg.eqs('QUANT_SCREENSPLITS')
            write_output(data, 'splits')
            break
        except:
            time.sleep(10)

    for i in range(10):
        try:
            data = blbg.eqs('QUANT_SCREENDVDS')  
            write_output(data, 'dividends')
            break
        except:
            time.sleep(10)

    blbg.stopSession()


if __name__ == '__main__':
    main()
