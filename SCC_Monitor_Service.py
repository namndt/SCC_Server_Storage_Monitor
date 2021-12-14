import win32serviceutil
import servicemanager
import win32service
import win32event
import socket


class ServiceBase(win32serviceutil.ServiceFramework):
    _svc_name_ = 'PythonWinservice'
    _svc_display_name_ = 'Python Service'
    _svc_description_ = 'Testing run python script as windows service'

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, *args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)

    def parse_command_line(cls):
        win32serviceutil.HandleCommandLine(cls)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        self.stop()
        win32event.SetEvent(self.hWaitStop)
        self.ReportServiceStatus(win32service.SERVICE_STOPPED)

    def SvcDoRun(self):
        self.ReportServiceStatus(win32service.SERVICE_START_PENDING)
        try:
            self.ReportServiceStatus(win32service.SERVICE_RUNNING)
            self.start()
            self.main()
            win32event.WaitForSingleObject(self.hWaitStop, win32event.INFINITE)
            servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE, servicemanager.PYS_SERVICE_STARTED, (self._svc_name_, ''))
        except Exception:
            self.SvcStop()

    def start(self):
        pass

    def stop(self):
        pass

    def main(self):
        self.ReportServiceStatus(win32service.SERVICE_RUNNING)
        pass


# Testing oneself module
if __name__ == '__main__':
    ServiceBase.parse_command_line(ServiceBase)
