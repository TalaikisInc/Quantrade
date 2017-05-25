1) pip install -r requiremenst.txt

2) MT4 platforms should have MLQ indicators loaded on some of H1 chart.

3) To start WebDAV Windows server, get NSSM:
https://nssm.cc/

for each folder:
    2) nssm.exe install WebDAV_1

    3) Enter path, ex.: C:\Users\Administrator\Anaconda2\Scripts\wsgidav.exe

    4) Enter StartupDirectory, ex.: C:\Users\Administrator\Anaconda2\Scripts

    3) Arguments, ex.: --host=<ip_addr> --port=8010 --root="C:\Users\Administrator\AppData\Roaming\MetaQuotes\Terminal\4CEA74113F9B03A091193EB928E38709\MQL4\Files" --config=C:\inetpub\wwwroot\quantrade\webdav.conf
