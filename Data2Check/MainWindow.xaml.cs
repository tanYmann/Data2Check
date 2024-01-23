using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
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
        private System.Timers.Timer timer;
        static DataTable Atradius = new DataTable();
        static string Path = @"c:\tmp\DataCheck\";
        static OdbcConnection OdbcSDL = new OdbcConnection("DSN=Parity_SDL;Pooling=true;");
        static OdbcConnection OdbcHBS = new OdbcConnection("DSN=Parity_HBS;Pooling=true;");
        static string preString { get; set; }
        static int Standort = 1;
        public OdbcConnection[] Connections = new OdbcConnection[] { OdbcSDL, OdbcHBS };
        static string ASCIIPath = @"\\bauer-gmbh.org\DFS\SL\PROALPHA\10. Team-DÜ, EDI, WF\Datenexport\transASCIIact\test\";
        static FileInfo DateFile = new FileInfo(Directory.GetCurrentDirectory() + "\\LastDate.txt");
        static CancellationTokenSource cancellation = new CancellationTokenSource();
        const string RegistryKeyString = "Data2Check";
        NotifyIcon notifyIcon;
        static int testCounter = 0;
        TimeSpan target = new TimeSpan(2, 30, 0);
        static int TestCounter { get => testCounter; }
        Operations operations = new Operations();
        SQLMethods queriesMethods = new SQLMethods();
        bool WaitTxt = false;
        DateTime Endtime = new DateTime();


        // MainWindoiw
        public MainWindow()
        {
            this.Endtime = SetEndTime(new TimeSpan(0, 2, 0));
            InitializeComponent();
            InitializeNotifyIcon();
            SetRegistryKey();

            do
            {
                CheckNetwork();
                SetText("Keine Verbindung!");
            }while(!CheckNetwork());

            Task.Run(async () => await ExportData());
           
        }

        //Initialisierung des Timers für die Zeit bis zur nächsten Ausführung
        private void InitializeTimer()
        {
            timer = new System.Timers.Timer
            {
                Interval = 1000, // Timer-Intervall in Millisekunden (hier 1 Sekunde)
                AutoReset = true
            };

            timer.Elapsed += TimerElapsed;
            timer.Start();

        }

        // Connectionary - Das Dictionary der OdbcConnections
        public Dictionary<int, OdbcConnection> connectionary = new Dictionary<int, OdbcConnection>()
        {
            {1,OdbcSDL },
            {2,OdbcHBS }
        };

        // Endzeit des Timers
        public DateTime SetEndTime(TimeSpan span)
        {
            DateTime now = DateTime.Now;
            now = now.AddHours(1);
            return now;
        }

        // Vergangene Zeit
        private void TimerElapsed(object sender, ElapsedEventArgs e)
        {
            var remainingTime = NextExecution(Endtime);
            App.Current.Dispatcher.Invoke(() => SetRemainingTime(remainingTime));

            if (remainingTime <= TimeSpan.Zero)
            {
                timer.Stop();
                Task.Run(async () => await ExportData());
                Endtime = SetEndTime(target);
            }
        }

        // Anzeige der verbleibenden Zeit bis zur nächsten Ausführung
        private void SetRemainingTime(TimeSpan remainingTime)
        {
            txtRemainingTime.Text = $"Zeit bis zum nächsten Export: {remainingTime.Hours}h {remainingTime.Minutes}m {remainingTime.Seconds}s";
        }

        // Zeitspanne bis zur nächsten Ausführung
        TimeSpan NextExecution(DateTime targetTime)
        {
            DateTime now = DateTime.Now;
            DateTime targetDateTime = new DateTime(now.Year, now.Month, now.Day, targetTime.Hour, targetTime.Minute, targetTime.Second);

            if (WaitTxt == false)
            {
                System.Windows.Application.Current.Dispatcher.Invoke(() =>
                {
                    SetText("Warten auf nächste Ausführung \n");
                    WaitTxt = true;
                });
            }
            else
            {
                System.Windows.Application.Current.Dispatcher.Invoke(() =>
                {
                    SetText(".");
                });
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

            SetText("Key in Registrierung geprüft.\n");
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

        // Prüfen der Netzwerkverbindung
        static bool CheckNetwork()
        {
            bool status;
            Ping ping = new Ping();

            try
            {
                PingReply reply = ping.Send("www.google.de");
                return reply.Status == IPStatus.Success;
            }
            catch (Exception x)
            {
                return false;
            }
        }

        // Prüfen der VPN-Verbindung
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

        // ODBC-Treiber prüfen
        bool checkOdbc(OdbcConnection connection)
        {
            if (connection.Driver == null)
            {
                Task.Delay(3000);
                checkOdbc(connection);
                return false;
            }
            else
            {
                return true;
            }
        }

        // Prüfen ob OdbcConnection offen
        bool CheckUp(OdbcConnection connection)
        {
            bool check = false;
            if (connection != null)
            {
                try
                {
                    if (connection.State == ConnectionState.Open)
                    {
                        connection.Close();
                    }

                    connection.Open();
                    check = true;
                    return check;
                }
                catch (Exception x)
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


        // Der Datenexport
        public async Task ExportData()
        {
            Standort = 1;
            CancellationToken token = cancellation.Token;
            DataTable dataTable = new DataTable();
            operations.FillAtradius(Atradius);

            foreach (OdbcConnection conn in Connections)
            {
                SetPreString(conn.ConnectionString);
                UpdateProgressBar(10);
                SetText("Vorbereitung abgeschlossen. \n");
                Dictionary<string, string> dict = new Dictionary<string, string>();
                try
                {
                    if (conn.State != ConnectionState.Open)
                    {
                        conn.Open();
                    }

                    if (conn.ConnectionString.Contains("SDL"))
                    {
                        dict = queriesMethods.QueriesSDL;
                    }
                    else if (conn.ConnectionString.Contains("HBS"))
                    {
                        dict = queriesMethods.QueriesHBS;
                    }
                    foreach (var key in dict)
                    {
                        dataTable = new DataTable(key.Key.ToString());

                        using (OdbcCommand command = new OdbcCommand(key.Value, conn))
                        {
                            dataTable.Load(command.ExecuteReader());
                            queriesMethods.Table2CSV(dataTable, ASCIIPath, preString);
                            SetText(preString + dataTable.TableName + " wurde exportiert. \n");
                            UpdateProgressBar(10);

                            await Task.Delay(300);
                        }
                    }
                }

                catch (Exception ex)
                {
                    SetText($"Error: {ex.Message}");
                }
                finally
                {
                    if (conn.State == ConnectionState.Open)
                    {
                        conn.Close();
                    }
                }

                int i = 0;
                UpdateProgressBar(10);
                UpdateProgressBar(10);
                Standort++;

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

            InitializeTimer();
        }
        
        

        // SQL-Selections
        public void SqlQueries(DataTable kunden, DataTable lieferanten,DataTable belege)
        {
            queriesMethods.Table2CSV(kunden, ASCIIPath, preString);
            queriesMethods.Table2CSV(lieferanten, ASCIIPath, preString);
            queriesMethods.Table2CSV(belege, ASCIIPath, preString);
        }

        //Connection wähklen anHand des Standortes
        public OdbcConnection GetConnection(int standort)
        {
            OdbcConnection conn = new OdbcConnection();

            if(standort == 1)
            {
               return OdbcSDL;
            }
            else if(standort == 2)
            {
                conn = OdbcHBS;
            }

            return conn;
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
        public void SetText(string text)
        {
            System.Windows.Application.Current.Dispatcher.Invoke(() =>
            {
                txtStatus.Text += text;
                txtStatus.BringIntoView();
            });           
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
                FileInfo shortcutFileInfo = new FileInfo(startupFolderPath + "\\Data2Check.lnk");

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

        //Button zum Anlegen des Shortcuts in Autostart
        private void ButtonCreateShortcut_Click(object sender, RoutedEventArgs e)
        {
            CreateShortcutInAutostart();
        }

        // Button zum manuellen Starten des Datenexports
        private void BtnStartClick(object sender, RoutedEventArgs e)
        {
            Task.Run(async() => await ExportData());
        }
        
        // Button zum Öffnen der Optionen
        private void ButtonOptions_Click(object sender, RoutedEventArgs e)
        {
            Window1 window1 = new Window1();

            if (window1.IsVisible)
            {
                window1.BringIntoView();
            }
            else
            {
                window1.Show();
            }
        }
    }
}

