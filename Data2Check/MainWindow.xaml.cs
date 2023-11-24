using System;
using System.Data;
using System.Data.Odbc;
using System.Drawing;
using System.IO;
using System.Net.NetworkInformation;
using System.ServiceProcess;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Forms;
using System.Windows.Media;
using Microsoft.Win32;

namespace Data2Check
{
    public partial class MainWindow : Window
    {
        static DataTable Atradius = new DataTable();
        static string Path = @"c:\tmp\DataCheck\";
        static OdbcConnection OdbcSDL = new OdbcConnection("DSN=Parity_SDL;Pooling=true;");
        static OdbcConnection OdbcHBS = new OdbcConnection("DSN=Parity_HBS;Pooling=true;");
        static string preString { get; set; }
        static int Standort = 1;
        static OdbcConnection[] Connections = new OdbcConnection[] { OdbcSDL, OdbcHBS };
        static string ASCIIPath = @"\\bauer-gmbh.org\DFS\SL\PROALPHA\10. Team-DÜ, EDI, WF\Datenexport\transASCIIact\";
        static FileInfo DateFile = new FileInfo(Directory.GetCurrentDirectory() + "\\LastDate.txt");
        static CancellationTokenSource cancellation = new CancellationTokenSource();
        const string RegistryKeyString = "Data2Check";
        NotifyIcon notifyIcon;
        static int testCounter = 0;
        TimeSpan target = new TimeSpan(2, 30, 0);
        static int TestCounter { get => testCounter; }
        
        async Task Main()
        {
            SetRegistryKey();
            if (!await CheckNetwork())
            {
                await CheckNetwork();
            }
        }


        public MainWindow()
        {
            InitializeComponent();
            InitializeNotifyIcon();
            Task.Run(() => ExportData());
        }
            
        

        private void TimerElapsed(object sender, ElapsedEventArgs e)
        {
             
        }
        //        
        //        while (true)
        //        {
        //  
        //  
        //            Task.Run(() => ExportData());
        //            target = target.Add(new TimeSpan(24, 0, 0));
        //        }

        TimeSpan NextExecution(TimeSpan targetTime)
        {
            DateTime now = DateTime.Now;
            DateTime targetDateTime = new DateTime(now.Year, now.Month, now.Day, targetTime.Hours, targetTime.Minutes, targetTime.Seconds);
         
            if (now > targetDateTime)
            {
                targetDateTime = targetDateTime.AddDays(1);
            }

            return targetDateTime - now;
        }
        
        // Setzen  des Statustext
        public string SetStatusTxt(string text)
        {
            string statusTxt = text;
            return statusTxt;
        }

        // Autostart überprüfen
        private void SetRegistryKey()
        {
            RegistryKey key = Registry.CurrentUser.OpenSubKey("SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Run", true);

            if (key.GetValue("Data2Check") == null)
            {
                key.SetValue(key.Name, System.Reflection.Assembly.GetExecutingAssembly().Location);
            }
        }
        
        // Letztes Datum Programmausführung  
        string GetLastDate()
        {
            string dateString = string.Empty;

            using (FileStream fstream = new FileStream(DateFile.FullName, FileMode.Open, FileAccess.ReadWrite))
            {
                using (StreamReader sreader = new StreamReader(fstream))
                {
                    sreader.BaseStream.Position = 0;
                    dateString = sreader.ReadLine();
                }
            }

            return dateString;
        }
        
        static void QueryTimeoutCallback(object state)
        {
            if (state is CancellationTokenSource cancellationTokenSource)
            {
                cancellationTokenSource.Cancel();
            }
        }

        static async Task<bool> CheckNetwork()
        {
            bool status;
            Ping ping = new Ping();
            
            try
            {
                PingReply reply = await ping.SendPingAsync("www.google.com", 8000);
                return reply.Status == IPStatus.Success;
            }
            catch(Exception x)
            {
                return false;
            }
        }
        void IsJuniperAndConnected()
        {
            bool isConnected = false;
            NetworkInterface[] networkInterfaces = NetworkInterface.GetAllNetworkInterfaces();

            for (int i = 0; i < networkInterfaces.Length; i++)
            {
                if (networkInterfaces[i].Description.Contains("Juniper Network"))
                {
                    while (networkInterfaces[i].OperationalStatus != OperationalStatus.Up)
                    {
                        Thread.Sleep(30000);
                    }

                    isConnected = true;
                }
            }

            if (!isConnected)
            {
                IsJuniperAndConnected();
            }
        }

        bool checkOdbc(OdbcConnection connection)
        {
            if (connection.Driver == null)
            {
                Thread.Sleep(30000);
                checkOdbc(connection);
                return false;
            }
            else
            {
                return true;
            }
        }

        bool CheckUp(OdbcConnection connection)
        {
            bool check = false;
            if(connection != null)
            {
                try
                {
                    if(connection.State == ConnectionState.Open)
                    {
                        connection.Close();
                    }

                    connection.Open();
                    check = true;
                    return check;
                }
                catch(Exception x)
                {
                    
                }
            }
            else
            {
                Thread.Sleep(5000);
                CheckUp(connection);
                return check;
            }

            return check;
        }

        // Exportmethode
        [STAThread]
        async private void ExportData()
        {
            CancellationToken token = cancellation.Token;
            Operations operations = new Operations();
            SQLMethods queriesMethods = new SQLMethods();
            operations.FillAtradius(Atradius);
            System.Threading.Timer queryTimeoutTimer = new System.Threading.Timer(QueryTimeoutCallback, cancellation, 600000, Timeout.Infinite);

            foreach (OdbcConnection conn in Connections)
            {
                using (conn)
                {
                    try
                    {
                        conn.Close();
                    }
                    catch (Exception x)
                    {

                    }

                    conn.Open();

                    SetPreString(conn.ConnectionString);
                    UpdateProgressBar(10);
                    //DataTable kunden = queriesMethods.GetKunden(operations.GetLastDate(), conn, Standort.ToString());
                    //queriesMethods.Table2CSV(kunden, Path, preString);
                    //SetText("DU_Kunden_neu exportiert \n"); 
                    UpdateProgressBar(10);
                    //DataTable lieferanten = queriesMethods.GetLieferanten(operations.GetLastDate(), conn, Standort.ToString());
                    //queriesMethods.Table2CSV(lieferanten, Path, preString);
                    //SetText("DU_Lieferanten_neu exportiert \n");
                    UpdateProgressBar(10);
                    DataTable kundenASCII = queriesMethods.GetKundenASCII(queriesMethods.GetDataKunden(Standort.ToString()), conn);
                    queriesMethods.Table2CSV(kundenASCII, ASCIIPath, preString);
                    UpdateProgressBar(10);
                    DataTable lieferantenASCII = queriesMethods.GetLieferantenASCII(queriesMethods.GetDataLieferanten(Standort.ToString()), conn);
                    queriesMethods.Table2CSV(lieferantenASCII, ASCIIPath, preString);
                    UpdateProgressBar(10);
                    await Task.Delay(5000);
                    DataTable belegeASCII = queriesMethods.GetBelegeASCII(conn);
                    queriesMethods.Table2CSV(belegeASCII, ASCIIPath, preString);
                }

                UpdateProgressBar(10);
                UpdateProgressBar(10);
                
                Standort++;

                //operations.SetLastDate(DateFile);
                testCounter++;

                using (FileStream stream = new FileStream(@"C:\tmp\Testlaeufe.txt", FileMode.OpenOrCreate, FileAccess.ReadWrite))
                using (StreamWriter writer = new StreamWriter(stream))
                {
                    writer.BaseStream.Position = 0;
                    writer.WriteLine("Testläufe absolviert : " + TestCounter.ToString());
                }

                System.Windows.Application.Current.Dispatcher.Invoke(() =>
                {
                    progressbar.Value = 0;
                    progressbar.BringIntoView();
                });
            }
        }

        static DateTime GetDateTime2Repeat()
        {
            // Testen
            DateTime dateTime = new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.Now.Day, DateTime.Now.Hour, DateTime.Now.Minute, 0);
            TimeSpan timeSpan = new TimeSpan(0, 2, 0);
            // Realbetrieb
            //DateTime dateTime = new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.Now.Day, 2, 30, 0);
            //TimeSpan timeSpan = new TimeSpan(24,0,0);
            
            _ = dateTime.Add(timeSpan);

            return dateTime;
        }

        // Notify-Icon beim Minimieren
        private void InitializeNotifyIcon()
        {
            notifyIcon = new NotifyIcon
            {
                Icon = SystemIcons.Application,
                Visible = true
            };

            notifyIcon.Click += NotifyIcon_Click;
        }

        // Click auf Icon
        private void NotifyIcon_Click(object sender, EventArgs e)
        {
            Show();
            WindowState = WindowState.Normal;
        }

        // Minimieren des Fensters
        protected override void OnStateChanged(EventArgs e)
        {
            base.OnStateChanged(e);

            if (WindowState == WindowState.Minimized)
            {
                Hide();
                notifyIcon.ShowBalloonTip(1000, "Anwendung minimiert", "Die Anwendung wurde minimiert.", ToolTipIcon.Info);
            }
        }

        // Schließen des Fensters
        protected override void OnClosing(System.ComponentModel.CancelEventArgs e)
        {
            base.OnClosing(e);
            // Icon bei Schließen des Fensters entfernen
            notifyIcon.Dispose();
        }

        // Methode zum Anzeigen des Status Text
        private void SetText(string text)
        {
            txtStatus.Text += text;
            txtStatus.BringIntoView();
        }

        // Standortkürzel für Dateinamen
        static void SetPreString(string connectionString)
        {
            preString = string.Empty;

            if (connectionString.Contains("_SDL"))
            {
                preString = "SDL_";
            }
            else if (connectionString.Contains("_HBS"))
            {
                preString = "HBS_";
            }
        }
        
        // Bewegen der ProgressBar
        private void UpdateProgressBar(int value)
        {
            System.Windows.Application.Current.Dispatcher.Invoke(() =>
            {
                progressbar.Value += value;
                progressbar.BringIntoView();
            });
        }

        // Shortcut im Autostart anlegen
        private void CreateShortcutInAutostart()
        {
            try
            {
                string startupFolderPath = Environment.GetFolderPath(Environment.SpecialFolder.Startup);
                FileInfo appFileInfo = new FileInfo(System.Reflection.Assembly.GetExecutingAssembly().Location);
                FileInfo shortcutFileInfo = new FileInfo(startupFolderPath+"\\Data2Check.lnk");
                
                if (shortcutFileInfo.Exists)
                {
                    shortcutFileInfo.Delete();
                }
                
                using (StreamWriter writer = new StreamWriter(shortcutFileInfo.FullName))
                {
                    writer.WriteLine("[Data2Check]");
                    writer.WriteLine("URL=file:///" + appFileInfo.FullName.Replace('\\', '/'));
                    writer.WriteLine("IconIndex=0");
                    writer.WriteLine("IconFile=" + appFileInfo.FullName);
                }
            }
            catch (Exception ex)
            {
                System.Windows.MessageBox.Show("Fehler : " + ex.Message);
            }
        }

        private void ButtonCreateShortcut_Click(object sender, RoutedEventArgs e)
        {
            CreateShortcutInAutostart();
        }

        private void BtnStartClick(object sender, RoutedEventArgs e)
        {
            Task.Run(() => ExportData());
        }

        private void ButtonOptions_Click(object sender, RoutedEventArgs e)
        {
    
        }
    
        
    }
}

