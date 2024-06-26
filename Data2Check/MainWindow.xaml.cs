using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.IO;
using System.Linq;
using System.Net.NetworkInformation;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using System.Windows;
using System.Windows.Forms;

namespace Data2Check
{
    public partial class MainWindow : Window
    {
        private SQLMethods methods = new SQLMethods();
        private System.Timers.Timer timer = new System.Timers.Timer();
        private static DataTable Atradius = new DataTable("Atradius");
        private static OdbcConnection OdbcSDL = new OdbcConnection("DSN=Parity_SDL;Pooling=true;");
        private static OdbcConnection OdbcHBS = new OdbcConnection("DSN=Parity_HBS;Pooling=true;");
        private static string preString { get; set; }
        private static int Standort = 1;
        private OdbcConnection[] Connections = new OdbcConnection[] { OdbcSDL, OdbcHBS };
        private static string ASCIIPath = @"\\bauer-gmbh.org\DFS\SL\PROALPHA\10. Team-DÜ, EDI, WF\Datenexport\transASCIIact\";
        private static string DateFile = @"\\bauer-gmbh.org\DFS\SL\PROALPHA\10. Team-DÜ, EDI, WF\Datenexport\KundenLieferanten\LastDate.txt";
        private static CancellationTokenSource cancellation = new CancellationTokenSource();
        private const string RegistryKeyString = "Data2Check";
        private NotifyIcon notifyIcon;
        private static int testCounter = 0;
        private TimeSpan target = new TimeSpan(2, 30, 0);
        private static Operations operations = new Operations();
        private bool WaitTxt = false;
        private static readonly DateTime dateTime = new DateTime();
        private DateTime Endtime = dateTime;
        private static string LogFile = @"\\bauer-gmbh.org\DFS\SL\PROALPHA\10. Team-DÜ, EDI, WF\Datenexport\Logger.txt";
        private Window1 Window1 = new Window1();
        DataTable kobensen = new DataTable("Kobensen");
        ProgressBar progressBar = new ProgressBar();
        private System.Timers.Timer countdownTimer;
        private DateTime nextExecutionTime;
        TimerWindow timerWindow = new TimerWindow();
        public async Task MainAsync()
        {
            while (!CheckNetwork())
            {
                SetText("Netzwerkverbindung nicht bereit. Warte 5 Sekunden...");
                await Task.Delay(5000); // Fügt eine asynchrone Pause von 5 Sekunden hinzu
            }
            SetText("Netzwerkverbindung ist jetzt bereit.");
        }

        public MainWindow()
        {
            operations.FillAtradius(Atradius);
            operations.FillUstidKobensen(kobensen);
            InitializeComponent();
            SetRegistryKey();
            SetText("Anwendung gestartet.\n");
            SetText("Netzwerkverbindung wird geprüft.\n");
            SetText("VPN-Verbindung wird geprüft.\n");
            SetText("ODBC-Treiber wird geprüft.\n");
            SetText("ODBC-Verbindung wird geprüft.\n");
            SetText("Daten werden exportiert.\n");
            Task.Run(() => ExecuteQueries(ExportData()));
        }

        private void WriteLogFile(string line)
        {
            using (StreamWriter writer = File.AppendText(LogFile))
            {
                writer.WriteLine($"{DateTime.Now.Date} : [{DateTime.Now.Hour}:{DateTime.Now.Minute} : {line}");
            }
        }

        private DateTime SetEndTime(TimeSpan span)
        {
            return DateTime.Now + span;
        }

        private void SetRegistryKey()
        {
            RegistryKey key = Registry.CurrentUser.OpenSubKey("SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Run", true);

            if (key.GetValue("Data2Check") == null)
            {
                key.SetValue(key.Name, System.Reflection.Assembly.GetExecutingAssembly().Location);
            }

            SetText("Key in Registrierung geprüft.\n");
        }

        private (string Date2Start, string DateToEnd) GetLastDate()
        {
            DateTime dtEnd = DateTime.Now;
            DateTime dtStart = DateTime.Now.AddDays(-4);

            string dateEnd = dtEnd.ToString("yyyyMMdd");
            string dateStart = dtStart.ToString("yyyyMMdd");

            return (dateStart, dateEnd);
        }

        private bool CheckNetwork()
        {
            try
            {
                Ping ping = new Ping();
                PingReply reply = ping.Send("www.google.de");
                return reply.Status == IPStatus.Success;
            }
            catch (Exception)
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

        private void WriteLog(string line)
        {
            if (!Directory.Exists(@"c:\tmp\logs"))
            {
                Directory.CreateDirectory(@"c:\tmp\logs");
            }

            if (!File.Exists(@"c:\tmp\logs\D2CLog.txt"))
            {
                File.Create(@"c:\tmp\logs\D2CLog.txt");
            }
            using (StreamWriter writer = File.AppendText(@"c:\tmp\logs\D2CLog.txt"))
            {
                writer.WriteLine($"{DateTime.Now} : {line}");
            }
        }

        async Task<bool> checkOdbc(OdbcConnection connection)
        {
            if (connection.Driver == null)
            {
                await Task.Delay(3000);
                await checkOdbc(connection);
                return false;
            }
            else
            {
                return true;
            }
        }

        private void SetupCountdownTimer()
        {
            TimerWindow timerWindow = new TimerWindow();
            // Setzen Sie nextExecutionTime auf die gewünschte Zeit der nächsten Ausführung
            nextExecutionTime = DateTime.Now.AddMinutes(3); // Beispiel: Nächste Ausführung in einer Stunde

            countdownTimer = new System.Timers.Timer(1000); // Timer aktualisiert jede Sekunde
            countdownTimer.Elapsed += OnCountdownTimerElapsed;
            countdownTimer.AutoReset = true;
            countdownTimer.Enabled = true;
        }

        private void OnCountdownTimerElapsed(object sender, ElapsedEventArgs e)
        {

            // Berechnung der verbleibenden Zeit bis zur nächsten Ausführung
            TimeSpan remainingTime = nextExecutionTime - DateTime.Now;

            // Aktualisieren Sie hier die Benutzeroberfläche mit der verbleibenden Zeit
            // Da Timer-Callbacks in einem anderen Thread ausgeführt werden, müssen Sie sicherstellen,
            // dass die Aktualisierung der Benutzeroberfläche im UI-Thread erfolgt.
            this.Dispatcher.Invoke(() =>
            {
                // Beispiel: Aktualisieren eines Labels mit der verbleibenden Zeit
                // Ersetzen Sie "timeLabel" durch den tatsächlichen Namen Ihres Labels
                timerWindow.timeLabel.Content = $"Verbleibende Zeit: {remainingTime.Hours} Stunden, {remainingTime.Minutes} Minuten, {remainingTime.Seconds} Sekunden";
            });
            // Wenn die verbleibende Zeit abgelaufen ist, führen Sie die gewünschte Aktion aus
            if (remainingTime <= TimeSpan.Zero)
            {
                countdownTimer.Stop();
                OdbcConnection connection = new OdbcConnection("DSN=Parity_SDL;Pooling=true;");
                // Führen Sie hier die gewünschte Aktion aus
                // Beispiel: Starten Sie eine Methode, die die Aufgabe ausführt
                // Ersetzen Sie "StartTask" durch den Namen der Methode, die die Aufgabe ausführt


                Task.Run(() => BelegeASCIITable());

                GetDataKunden();
                GetDataLieferanten();

                // Setzen Sie nextExecutionTime auf die gewünschte Zeit der nächsten Ausführung
                nextExecutionTime = DateTime.Now.AddHours(1); // Beispiel: Nächste Ausführung in einer Stunde
                countdownTimer.Start();

            }
        }
        private void WriteDataTableToCsv(DataTable dataTable, string fileName)
        {
            StringBuilder sb = new StringBuilder();

            IEnumerable<string> columnNames = dataTable.Columns.Cast<DataColumn>().Select(column => column.ColumnName);
            sb.AppendLine(string.Join(";", columnNames));

            foreach (DataRow row in dataTable.Rows)
            {
                IEnumerable<string> fields = row.ItemArray.Select(field => field.ToString());
                sb.AppendLine(string.Join(";", fields));
            }

            File.WriteAllText(fileName, sb.ToString());
        }

        private void BtnCloseClick(object sender, RoutedEventArgs e)
        {
            Close();
        }

        private void InitializeTimer()
        {

            timer.Elapsed += TimerElapsed;
            timer.Interval = 1000;

            timer.Start();
        }

        public void StartTimer()
        {
            target = NextExecution(DateTime.Now);
            SetRemainingTime(target - DateTime.Now.TimeOfDay);
            InitializeTimer();
        }

        private void TimerElapsed(object sender, ElapsedEventArgs e)
        {

            if (DateTime.Now.TimeOfDay >= target)
            {
                timer.Stop();
                SetText("Timer elapsed. Exporting data...\n");
                Task.Run(() => ExecuteQueries(ExportData()));
                target = NextExecution(DateTime.Now);
                SetRemainingTime(target - DateTime.Now.TimeOfDay);
                timer.Start();
            }
            else
            {
                SetRemainingTime(target - DateTime.Now.TimeOfDay);
            }

        }

        public TimeSpan NextExecution(DateTime targetTime)
        {
            DateTime now = DateTime.Now;
            DateTime nextExecution = targetTime.Date.Add(targetTime.TimeOfDay);

            if (now > nextExecution)
            {
                nextExecution = nextExecution.AddDays(1);
            }

            return nextExecution - now;
        }

        public void SetRemainingTime(TimeSpan remainingTime)
        {
            string remainingTimeString = remainingTime.ToString(@"hh\:mm\:ss");
            SetText($"Remaining time: {remainingTimeString}\n");
        }

        Dictionary<string, DataTable> SetTableDict(Dictionary<string, string> dictionary)
        {
            Dictionary<string, DataTable> tableDict = new Dictionary<string, DataTable>();

            foreach (KeyValuePair<string, string> kvp in dictionary)
            {
                DataTable dataTable = new DataTable(kvp.Key);
                dataTable = methods.GetTable(kvp.Value, OdbcSDL, kvp.Key);
                tableDict.Add(kvp.Key, dataTable);
            }

            return tableDict;
        }

        override protected void OnStateChanged(EventArgs e)
        {
            if (WindowState == WindowState.Minimized)
            {
                Hide();
            }

            base.OnStateChanged(e);
        }

        protected override void OnClosing(System.ComponentModel.CancelEventArgs e)
        {
            e.Cancel = true;
            WindowState = WindowState.Minimized;
        }

        void SetText(string text)
        {
            Dispatcher.Invoke(() =>
            {
                txtStatus.Text += text + "\n";
            });
        }

        public DataTable BelegeASCIITable()
        {
            dataTable = new DataTable("SDL_Belege_ASCII");

            string query = "select " +
        "bel_nr," +
        "bel_zbnr," +
        "kdn_kontonr," +
        "his_renr," +
        "pkt_rgzahler " +
        "from beleg,kunden,historie,persktn " +
        "where  kdn_kontonr = bel_kontonr " +
        "and his_kdnlfdnr = kdn_lfdnr " +
        "and pkt_ktonr = kdn_kontonr " +
        "and his_belnr = bel_nr " +
        "and his_redat > 20231231 " +
        "group by his_renr";


            OdbcCommand command = new OdbcCommand(query, connection);
            dataTable = WriteTable(command, dataTable);

            return dataTable;
        }

        //----Einzelkunde ASCII
        public void GetDataKundenEinzeln()
        {
            string query = string.Empty;

            dataTable = new DataTable("Kunden_ASCII");
            if (standort == "1")
            {
                query = "select " +
                    "kdn_kontonr," +
                    "kdn_zbnr," +
                    "ans_ustid," +
                    "kdn_ekvnr, " +
                    "(select f1e_x_ekname from f1ekverband where f1e_x_eknr = kdn_ekvnr limit 1)  " +
                    "from kunden,anschrift " +
                    "where kdn_lfdnr = ansnr " +
                    "and kdn_typ = 'D' ";
            }
        }

        //------------------Kundenndaten für transASCIIact
        public void GetDataKunden()
        {
            string query = string.Empty;

            dataTable = new DataTable("Kunden_ASCII");
            if (standort == "1")
            {
                query = "select " +
                    "distinct kdn_kontonr," +
                    "kdn_zbnr," +
                    "ans_ustid," +
                    "kdn_ekvnr, " +
                    "(select f1e_x_ekname from f1ekverband where f1e_x_eknr = kdn_ekvnr limit 1)  " +
                    "from kunden,anschrift " +
                    "where kdn_lfdnr = ansnr " +
                    "and kdn_typ = 'D' " +
                    "group by kdn_kontonr";
            }

            if (standort == "2")
            {
                query = "select " +
                   "distinct kdn_kontonr," +
                   "kdn_zbnr," +
                   "ans_ustid," +
                   "kdn_ekvnr, " +
                   "(select f1e_x_ekname from f1ekverband where f1e_x_eknr = kdn_ekvnr limit 1) AS f1ekname " +
                   "from kunden,anschrift " +
                   "where kdn_lfdnr = ansnr " +
                   "and kdn_typ = 'D'";
            }

            OdbcCommand command = new OdbcCommand(query, connection);
            dataTable = WriteTable(command, dataTable);
            Table2CSV(dataTable);
        }

        //------------------Lieferantendaten für transASCIIact
        public void GetDataLieferanten()
        {
            dataTable = new DataTable("Lieferanten_ASCII");
            string query = string.Empty;

            if (standort == "1")
            {
                query = "select " +
                    "kdn_kontonr," +
                    "name_001," +
                    "land," +
                    "kdn_zbnr," +
                    "ans_ustid " +
                    "from kunden,anschrift " +
                    "where kdn_lfdnr = ansnr " +
                    "and kdn_kontonr not like ('82013') " +
                    "and kdn_info_001 not like ('gelöscht') " +
                    "and kdn_typ = 'K' " +
                    "group by kdn_kontonr ";
            }

            if (standort == "2")
            {
                query = "select " +
                    "kdn_kontonr," +
                    "name_001," +
                    "land," +
                    "kdn_zbnr," +
                    "ans_ustid " +
                    "from kunden,anschrift " +
                    "where kdn_lfdnr = ansnr " +
                    "and kdn_typ = 'K' " +
                    "and kdn_kontonr not like '99999' " +
                    "group by kdn_kontonr ";
            }

            try
            {
                OdbcCommand command = new OdbcCommand(query, connection);
                dataTable = WriteTable(command, dataTable);
                Table2CSV(dataTable);
            }
            catch (Exception x)
            {
                Logger("[" + DateTime.Now.Day + "." + DateTime.Now.Month + "." + DateTime.Now.Year + "] \n" +
                       "Message : \n " +
                       "\n" +
                       x.Message + "\n " +
                       "Target : \n " +
                       "\n" +
                       x.TargetSite + "\n" +
                       "\n" +
                       "Data : " +
                       "\n" +
                       x.Data + "\n" +
                       "Source : \n" +
                       "\n" +
                       x.Source + "\n");
            }
        }

        void CreateShortcutInAutostart()
        {
            string shortcutPath = Environment.GetFolderPath(Environment.SpecialFolder.Startup) + "\\Data2Check.lnk";
            string targetPath = System.Reflection.Assembly.GetExecutingAssembly().Location;

            if (!File.Exists(shortcutPath))
            {
                shortcutPath = shortcutPath +
                    "\\Data2Check.lnk";
            }
        }

        void ButtonCreateShortcut_Click(object sender, RoutedEventArgs e)
        {
            CreateShortcutInAutostart();
        }

        void BtnStartClick(object sender, RoutedEventArgs e)
        {
            StartTimer();
        }

        void ButtonOptions_Click(object sender, RoutedEventArgs e)
        {
            Window1.Show();
        }
    }
}
