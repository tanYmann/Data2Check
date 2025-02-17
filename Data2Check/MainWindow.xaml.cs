using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.IO;
using System.Net.NetworkInformation;
using System.Text.RegularExpressions;
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
        //TimerWindow timerWindow = new TimerWindow();
        DataTable dataTable = new DataTable();
        private MainWindow Window = App.Current.MainWindow as MainWindow;
        string[] Standorte = new string[] { "1", "2" };
        int laeufe = 0;
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

            Task.Run(() => SetupCountdownTimer());
        }

        public string GetNrVerb(string name, string name2, string vbNr)
        {
            string pattern = @"\d*\d";

            Regex regex = new Regex(pattern);

            if (vbNr == "119516")
            {
                name = name.Replace(" ", "");

                if (name.Contains("L345776M"))
                {
                    name = name.Replace("L345776M", "");
                }
                else
                {
                    name = name2;
                }

                name = name.Replace(" ", "");
                name = name.Replace("L345776M", "");
                name = regex.Match(name).Value.ToString();
            }
            else if (vbNr == "138145")
            {
                string nordwest = "Nordwest";

                if (!name.Contains(nordwest))
                {
                    name = name2;
                }
                try
                {
                    name = name.Substring(0, name.LastIndexOf('-'));
                    name = regex.Match(name).Value.ToString();
                }

                catch (Exception e)
                {
                    if (e.InnerException != null)
                    {
                        WriteLogFile(e.InnerException.Message);
                    }
                    else
                    {
                        WriteLogFile(e.Message);
                    }
                    name = "";
                }
                name = "";

            }
            else if (vbNr == "119433")
            {
                string evb = "##";

                if (!name.Contains(evb))
                {
                    name = name2;
                }

                name = regex.Match(name).Value.ToString();
            }
            else if (vbNr == "119693")
            {
                string evb = "ZR0135VL74171AH";

                if (!name.Contains("0135"))
                {
                    name = name2;
                }

                name = name.Replace(" ", "");
                name = name.Replace(evb, "");
                name = regex.Match(name).Value.ToString();
            }

            return name;
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
            nextExecutionTime = DateTime.Now.AddSeconds(10);

            countdownTimer = new System.Timers.Timer(1000);
            countdownTimer.Elapsed += OnCountdownTimerElapsed;
            countdownTimer.AutoReset = true;
            countdownTimer.Enabled = true;
        }

        private void OnCountdownTimerElapsed(object sender, ElapsedEventArgs e)
        {

            TimeSpan remainingTime = nextExecutionTime - DateTime.Now;

            this.Dispatcher.Invoke(() =>
            {
                txtRemainingTime.Text = $"Verbleibende Zeit: {remainingTime.Hours} Stunden, {remainingTime.Minutes} Minuten, {remainingTime.Seconds} Sekunden";
            });

            if (remainingTime <= TimeSpan.Zero)
            {
                countdownTimer.Stop();

                //Export der Daten
                Table2CSV(BelegeASCIITable(), "SDL_Belege_ASCII.csv", "");
                Table2CSV(GetDataKunden(), "SDL_Kunden_ASCII.csv", "");
                Table2CSV(GetDataLieferantenSDL(), "SDL_Lieferanten_ASCII.csv", "");
                Table2CSV(GetDataLieferantenHBS(), "HBS_Lieferanten_ASCII.csv", "");
                Table2CSV(GetKunden(), "SDL_Kundenimport.csv", "K");

                foreach (var standort in Standorte)
                {
                    string index = "";

                    if (standort == "1")
                    {
                        index = "SDL";
                    }
                    else
                    {
                        index = "HBS";
                    }

                    Table2CSV(GetLieferant(standort), index + "_Lieferanten.csv", "L");
                    SetText(index + " Lieferantendaten wurden exportiert.\n");

                }

                //Nächste Ausführung um 8:50 Uhr
                nextExecutionTime = DateTime.Today.AddHours(9).AddMinutes(2);

                if (nextExecutionTime < DateTime.Now)
                {
                    //Nächste Ausführung um 8:50 Uhr am nächsten Tag
                    nextExecutionTime = nextExecutionTime.AddDays(1);
                }

                countdownTimer.Start();
            }
        }
        private void Table2CSV(DataTable table, string fileName, string index)
        {
            FileInfo pathFileInfo1 = null;
            FileInfo pathFileInfo = new FileInfo(ASCIIPath + "\\" + fileName);
            int i = 0;


            if (index == "K")
            {
                pathFileInfo = new FileInfo(@"\\bauer-gmbh.org\DFS\SL\PROALPHA\10. Team-DÜ, EDI, WF\06 Datenimport\Autoimport\93\Test1\Kundendaten\" + fileName);
                pathFileInfo1 = new FileInfo(@"\\bauer-gmbh.org\DFS\SL\PROALPHA\10. Team-DÜ, EDI, WF\06 Datenimport\Autoimport\93\Production\Kundendaten\" + fileName);
            }

            if (index == "L")
            {
                pathFileInfo = new FileInfo(@"\\bauer-gmbh.org\\DFS\\SL\\PROALPHA\\10. Team-DÜ, EDI, WF\\06 Datenimport\\Autoimport\\93\\Test1\\Lieferanten\\" + fileName);
                pathFileInfo1 = new FileInfo(@"\\bauer-gmbh.org\\DFS\\SL\\PROALPHA\\10. Team-DÜ, EDI, WF\\06 Datenimport\\Autoimport\\93\\Production\\Lieferanten\\" + fileName);
            }

            try
            {
                if (pathFileInfo.Exists)
                {
                    pathFileInfo.Delete();
                }

                WriteFile(pathFileInfo, table);
                SetText("Datei " + fileName + " wurde exportiert.\n");

                if (pathFileInfo1 != null)
                {
                    if (pathFileInfo1.Exists)
                    {
                        pathFileInfo1.Delete();
                    }

                    WriteFile(pathFileInfo1, table);
                    SetText("Datei " + fileName + " wurde exportiert.\n");
                }
            }
            catch (Exception e)
            {
                Dispatcher.Invoke(() =>
                {
                    SetText(e.Message);
                    WriteLogFile(e.Message);
                });

            }
        }

        void WriteFile(FileInfo pathFileInfo, DataTable table)
        {
            using (FileStream fs = new FileStream(pathFileInfo.FullName, FileMode.OpenOrCreate, FileAccess.Write))
            using (StreamWriter streamWriter = new StreamWriter(fs, System.Text.Encoding.UTF8))
            {
                string cols = "";
                if (table.Columns.Count > 0)
                {
                    foreach (DataColumn column in table.Columns)
                    {
                        cols += column.ColumnName.ToString() + ";";
                    }

                    cols = cols.Substring(0, cols.Length - 1);
                    streamWriter.WriteLine(cols);

                    foreach (DataRow row in table.Rows)
                    {
                        string line = "";

                        foreach (var entry in row.ItemArray)
                        {
                            line += entry.ToString() + ";";
                        }

                        line = line.Substring(0, line.Length - 1);
                        streamWriter.WriteLine(line);
                    }
                }
            }
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
            OdbcConnection connection = new OdbcConnection("DSN=Parity_SDL;Pooling=true;");
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
            dataTable = operations.WriteTable(command, dataTable, connection);
            SetText("Belege wurden exportiert.\n");
            return dataTable;
        }

        //------------------Kundenndaten für transASCIIact
        public DataTable GetDataKunden()
        {
            string query = string.Empty;

            dataTable = new DataTable("Kunden_ASCII");
            OdbcConnection connection = new OdbcConnection("DSN=Parity_SDL;Pooling=true;");
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

            OdbcCommand command = new OdbcCommand(query, connection);
            dataTable = operations.WriteTable(command, dataTable, connection);
            SetText("Kundendaten SDL wurden exportiert.\n");
            return dataTable;
        }

        //------------------Lieferantendaten für transASCIIact
        public DataTable GetDataLieferantenSDL()
        {
            Standort = 1;
            dataTable = new DataTable("Lieferanten_ASCII");
            OdbcConnection connection = new OdbcConnection("DSN=Parity_SDL;Pooling=true;");
            string query = "select " +
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

            OdbcCommand command = new OdbcCommand(query, connection);
            dataTable = operations.WriteTable(command, dataTable, connection);
            Table2CSV(dataTable, "SDL_" + dataTable.TableName, "");

            SetText("Lieferantendaten SDL wurden exportiert.\n");
            return dataTable;

        }

        public DataTable GetDataLieferantenHBS()
        {
            Standort = 2;
            dataTable = new DataTable("Lieferanten_ASCII");
            OdbcConnection connection = new OdbcConnection("DSN=Parity_HBS;Pooling=true;");

            string query = "select " +
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

            OdbcCommand command = new OdbcCommand(query, connection);
            dataTable = operations.WriteTable(command, dataTable, connection);
            Table2CSV(dataTable, "HBS_" + dataTable.TableName, "");
            SetText("Lieferantendaten HBS wurden exportiert.\n");
            return dataTable;
        }

        public DataTable GetKunden()
        {
            string endDate = string.Empty;
            string startDate = string.Empty;
            endDate = DateTime.Now.AddDays(1).ToString("yyyyMMdd");
            startDate = DateTime.Now.AddDays(-4).ToString("yyyyMMdd");
            List<string> rgzahler = new List<string>();
            dataTable = new DataTable("Kundendaten__9_3");
            OdbcConnection connection = new OdbcConnection("DSN=Parity_SDL;Pooling=true;");
            Standort = 1;
            string selectCommand2 = "SELECT " +
                    "kunden.kdn_kontonr," +
                    "'', " +
                    "anschrift.name_001, " +
                    "anschrift.land, " +
                    "anschrift.ort, " +
                    "anschrift.anssuch, " +
                    "anschrift.ans_suwo2, " +
                    "'@', " +
                    "'@'," +
                    "'@', " +                              //09
                    "'@', " +
                    "anschrift.name_002," +                //11
                    "anschrift.name_003, " +
                    "anschrift.plz," +
                    " '@' AS Expr3, " +
                    "anschrift.ans_teilort, " +
                    "'@' AS Expr4, " +
                    "anschrift.strasse, " +
                    "'@' AS Expr5, " +
                    "'Hausnummer' AS Hausnummer," +
                    "'@' AS Bundesland, " +
                    "anschrift.ans_pf_plz, " +
                    "anschrift.ans_postfach, " +
                    "'Ja' AS Expr8, " +
                    "anschrift.ans_email, " +
                    "anschrift.ans_homepage, " +
                    "anschrift.ans_telex, " +
                    "anschrift.ans_telefon, " +
                    "'' as 'Autotelefon'," +             //28
                    "anschrift.ans_telefax, " +
                    "'' as 'Telefon2'," +
                    "'' as 'Telefax2'," +
                    "'' as 'Längengrad'," +
                    "'' as 'Breitengrad'," +
                    "anschrift.anssuch AS Expr10, " +
                    "anschrift.ans_suwo2 AS Expr11, " +
                    "'VK'," +
                    "kunden.kdn_x_branche, " +
                    "'@' AS 'Region'," +               //38
                    "'@', " +
                    "kdn_sprnr, " +                   //40
                    "'' as 'ABC-Klasse'," +
                    "'@' AS 'Teilestatistik', " +
                    "'@' AS 'Webshop', " +
                    "'@' AS 'Bestandsfaktor', " +
                    "'@' AS 'Lagerort', " +
                    "kunden.kdn_erstellt, " +

                   //"'' as 'Nummer bei Lieferant', " +            //48
                   "persktn.pkt_kdn_lief_nr ," +


                    "'', " +          //48
                    "'0'," +
                    "anschrift.ans_ustid, " +         //50
                    "'@' AS 'Rechnungsintervall', " +
                    "kunden.kdn_faktkz, " +

                    //"'' as 'Rechnungszahler', " +
                    "persktn.pkt_rgzahler, " +                      //53


                    "'@' AS  'Konzern', " +
                    "'@' AS 'Bezeichnung', " +
                    "kunden.kdn_ekvnr, " +                          //56          
                    "(select f1e_x_ekname from f1ekverband where f1ekverband.f1e_x_eknr = kunden.kdn_ekvnr limit 1) AS f1ekname, " +
                    "'@' AS Expr24, " +                             //55
                    "'@' AS Expr25, " +
                    "'@' AS Expr26, " +
                    "'@' AS Expr27, " +
                    "'@' AS Expr28, " +
                    "'1', " +                                       //63
                    "'2', " +
                    "'3', " +
                    "'4', " +
                    "'5', " +
                    "'6', " +
                    "'3' as 'Zahlungsart', " +                      //69
                    "'@' AS Expr36, " +
                    "kdn_zbnr, " +                                  //71    
                    "kunden.kdn_kredlimit, " +                      //72
                    "'JA', " +                             //73
                    "'2' AS 'Mahnverfahren', " +                    //74
                    "'@' AS Expr39, " +                             //75
                    "'@' AS Expr40, " +                             //76
                    "'@' AS Expr41, " +                             //77
                    "kunden.kdn_vsanr, " +                          //78
                    "kdn_lbdnr," +                                        //79
                    "'1' as 'Lieferrestriktion',  " +               //80    
                    "'' as 'Vertrags'," +                                          //81
                    "'@' AS 'Risikonummer WKV', " +

                    //"'' as 'Kreditversicherung', " +
                    "pkt_kredvers, " +                       //83

                    "'@' AS 'WKV-Kennzeichen', " +
                    "'@' as 'l. Auskunft', " +
                    "kunden.kdn_x_adrsel, " +                       //86
                    "kdn_x_adrsel||'_'||anschrift.land||'_'||kdn_vertrnr_001, " +//87
                    "'JA', " +                         //88
                    "'Ja' AS Expr50, " +                           //89
                    "'Ja' AS Expr51, " +                           //90
                    "'@' AS Expr52, " +                            //91 
                    "'0'as 'Sammellieferschein', " +               //92 
                    "kdn_x_ust_wann," +                            //93
                    "'' as 'Bankverbindung'," +
                    "'Ja' as 'Default'," +
                    "'@' ," +
                    "'1' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "kdn_rekopie," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' as 'Versandkalender'," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@', " +
                    "'@', " +
                    "'@' " +           //146
                    "FROM kunden, anschrift " +
                    ",persktn " +
                    ",historie " +
                    "WHERE (anschrift.ansnr = kunden.kdn_lfdnr) " +
                    "AND (persktn.pkt_ansnr = anschrift.ansnr) " +
                    "and his_kdnlfdnr = kdn_lfdnr " +
                    "and kdn_typ = 'D' " +
            //"and kdn_kontonr in (10020,10031,10048,10065,10080,10083,10093,11001,11007,11106,11132,11232,11243,11246,11274,11281,11303,11332,11363,11415,11429,11447,11452,11459,11465,11467,11485,11523,11527,11563,11601,11692,11697,11731,11783,11787,11820,11860,11927,11929,11930,11938,11939,11960,11965,11990,11993,12016,12090,12098,12143,12169,12174,12179,12180,12185,12186,12195,12201,12225,12236,12264,12281,12298,12304,12316,12382,12388,12396,12404,12405,12441,12451,12510,12526,12531,12537,12543,12564,12565,12569,12573,12576,12595,12604,12607,12618,12638,12640,12654,12684,12709,12726,12739,12752,12775,12802,12805,12807,12814,12832,12849,12861,12880,12895,12898,12926,12977,12980,12990,12991,12993,12995,12998,13051,13064,13069,13073,13078,13087,13090,13092,13094,13095,13113,13178,13190,13191,13194,13210,13215,13218,13276,13309,13335,13364,13380,13384,13386,13402,13403,13435,13472,13554,13559,13572,13579,13586,13594,13606,13620,13629,13649,13652,13787,13817,13824,13835,13849,13853,13912,13932,13933,13938,14023,14079,14090,14116,14137,14184,14240,14263,14274,14396,14422,14431,14432,14487,14566,14626,14630,14641,14687,14690,14715,14731,14783,14786,14787,14801,14823,14827,14837,14844,14867,14887,14917,14933,14969,15049,15084,15115,15118,15134,15251,15272,15287,15362,15400,15420,15430,15453,15460,15463,15475,15521,15527,15558,15579,15587,15596,15603,15638,15685,15687,15693,15699,15700,15717,15733,15756,15763,15781,15784,15789,15811,15820,15823,15836,15851,15859,15895,15902,15906,15916,15921,15931,15936,15957,15960,15979,15982,15983,15991,16000,16001,16005,16012,16016,16019,16021,16025,16028,16029,16030,16043,16046,16047,16048,16049,16050,16051,16052,16053,16054,16055,16056,16057,16058,16059,16060,16061,16062,16063,16064,16065,16066,16067,16068,16069,16070,16071,16072,16073,16074,16075,16076,16077,16078,16079,16080,16081,16082,16083,16084,16085,16086,16087,16088,16089,16090,16091,16092,16093,16094,16095,16097,16098,16099,16100,16101,16102,16103,16104,16105,16108,16109,16110,16111,16113,16116,16117,16118,16119,16120,16122,16123,16124,16125,16129,17007,17013,17015,17055,17060,17092,17093,17161,17170,17178,17195,17219,17232,17246,17271,17364,17388,17398,17407,17415,17422,17431,17444,17489,17494,17533,17544,17555,17563,17615,17726,17798,17818,17836,17884,17897,17899,17946,17971,17981,17982,17992,18014,18034,18036,18048,18094,18117,18144,18160,18168,18196,18197,18257,18283,18287,18317,18346,18372,18373,18383,18389,18438,18448,18456,18474,18500,18501,18517,18523,18534,18554,18581,18587,18590,18591,18596,18608,18609,18612,18615,18621,18624,18625,18626,18627,18628,18629,18630,18631,18632,18634,18636,18637,19035,19049,19051,19058,19059,19064,19082,19088,19089,19097,19105,19129,19150,19174,19186,19198,19202,19209,19219,19243,19245,19301,19355,19392,19409,19433,19437,19454,19516,19569,19595,19613,19646,19677,19693,19721,19739,19744,19752,19779,19783,19791,19793,19796,19816,19822,19837,19851,19854,19860,19902,19909,19910,19927,19946,19947,19953,19957,19966,19969,19983,19995,20005,20006,20015,20021,20030,20036,20041,20059,20079,20108,20111,20112,20158,20161,20196,20225,20233,20239,20240,20255,20284,20287,20296,20302,20319,20325,20334,20367,20387,20388,20399,20403,20424,20445,20453,20473,20475,20485,20492,20493,20494,20524,20561,20562,20563,20570,20579,20597,20624,20629,20633,20640,20660,20664,20665,20666,20667,20670,20673,20681,20685,20711,20718,20721,20723,20724,20727,20728,20730,20731,20732,20733,20734,20737,20738,21030,21081,21118,21151,21159,21210,21220,21228,21248,21259,21309,21337,21341,21370,21443,21447,21462,21554,21611,21612,21641,21658,21668,21691,21722,21755,21831,21857,21859,21875,21878,21896,21901,21902,21910,21929,21948,21950,21986,22009,22010,22012,22023,22025,22026,22035,22052,22060,22073,22082,22089,22093,22105,22128,22130,22169,22189,22226,22230,22252,22256,22289,22303,22307,22309,22310,22353,22356,22375,22405,22426,22430,22439,22444,22445,22446,22449,22450,22451,22452,22458,22459,22470,22471,22472,22473,22474,22475,22476,22477,22478,22479,22481,22482,23015,23038,23053,23153,23163,23165,23174,23186,23206,23221,23245,23255,23284,23310,23317,23318,23371,23396,23398,23422,23427,23456,23471,23513,23531,23534,23600,23602,23620,23628,23660,23663,23689,23699,23724,23748,23924,23947,23955,23959,23963,23998,24040,24050,24051,24087,24090,24109,24114,24118,24130,24142,24151,24162,24186,24193,24235,24271,24280,24292,24305,24309,24337,24344,24365,24366,24385,24387,24390,24391,24405,24423,24444,24484,24493,24495,24511,24514,24519,24544,24561,24566,24574,24589,24596,24633,24648,24655,24670,24672,24676,24682,24683,24703,24709,24720,24737,24751,24776,24780,24790,24799,24802,24811,24818,24840,24846,24854,24867,24871,24875,24876,24883,24886,24887,24898,24899,24902,24906,24908,24910,24911,24912,24914,24915,24916,24917,24918,24919,24920,24921,24922,24924,24925,25012,25013,25032,25035,25066,25069,25156,25177,25181,25203,25215,25216,25230,25257,25270,25343,25358,25363,25412,25516,25526,25528,25531,25563,25571,25576,25603,25613,25622,25633,25670,25676,25682,25700,25713,25726,25759,25794,25815,25849,25866,25958,25966,25969,25975,26129,26138,26165,26181,26184,26187,26212,26223,26237,26247,26259,26267,26345,26359,26377,26391,26392,26403,26441,26449,26450,26471,26479,26508,26525,26536,26554,26557,26577,26585,26590,26593,26605,26640,26642,26649,26694,26699,26701,26704,26709,26716,26776,26778,26800,26812,26818,26832,26877,26881,26902,26916,26929,26990,27013,27046,27055,27058,27093,27095,27096,27101,27161,27249,27255,27263,27291,27292,27296,27304,27318,27322,27338,27351,27352,27357,27364,27372,27381,27399,27403,27410,27412,27455,27456,27463,27507,27511,27556,27568,27572,27583,27593,27599,27630,27664,27673,27679,27689,27715,27716,27725,27735,27742,27747,27750,27789,27794,27796,27805,27819,27825,27832,27840,27867,27868,27874,27925,27931,27965,27971,27975,27979,28014,28044,28106,28109,28130,28152,28216,28251,28294,28297,28317,28325,28408,28420,28443,28464,28469,28495,28507,28521,28541,28555,28556,28563,28564,28587,28621,28634,28635,28658,28660,28661,28667,28671,28676,28694,28695,28704,28708,28710,28711,28712,28713,28714,28715,28716,28717,29007,29012,29032,29041,29064,29065,29066,29068,29069,29070,29074,29076,29077,29079,29084,29098,29102,29137,29151,29153,29180,29182,29191,29209,29213,29215,29232,29240,29244,29246,29254,29277,29283,29292,29298,29301,29335,29339,29370,29388,29392,29395,29405,29406,29414,29437,29467,29474,29481,29500,29513,29514,29515,29537,29551,29563,29564,29566,30009,30017,30019,30022,30023,30025,30029,30084,30123,30157,30176,30184,30186,30193,30207,30209,30257,30307,30308,30318,30324,30367,30397,30417,30438,30461,30467,30473,30478,30483,30497,30503,30580,30594,30595,30598,30635,30655,30668,30684,30686,30687,30701,30710,30711,30725,30735,30739,30744,30757,30761,30775,30811,30918,30922,30929,30950,30954,30959,30967,31015,31024,31027,31048,31050,31059,31090,31094,31124,31155,31164,31170,31171,31179,31204,31212,31246,31260,31268,31270,31328,31344,31354,31364,31374,31385,31401,31421,31428,31434,31462,31464,31477,31483,31508,31540,31546,31553,31560,31627,31645,31655,31659,31664,31689,31691,31726,31741,31753,31824,31831,31841,31859,31860,31871,31879,31900,31912,31932,31952,31961,31983,31985,31994,32001,32031,32034,32046,32058,32061,32063,32104,32106,32113,32118,32134,32138,32140,32149,32167,32169,32185,32186,32188,32190,32193,32194,32217,32226,32237,32242,32243,32250,32254,32256,32271,32275,32301,32319,32329,32336,32391,32407,32437,32449,32471,32478,32486,32487,32490,32492,32512,32517,32528,32533,32539,32548,32552,32574,32595,32599,32605,32609,32632,32645,32649,32653,32654,32661,32666,32669,32674,32684,32696,32701,32703,32706,32718,32719,32722,32724,32727,32728,32731,32733,32736,32742,32744,32751,32759,32760,32762,32764,32770,32772,32774,32776,32777,32778,32779,32780,32781,32782,32783,32784,32786,33003,33032,33080,33105,33145,33171,33178,33188,33208,33210,33236,33244,33265,33314,33316,33343,33407,33424,33455,33459,33463,33486,33528,33532,33564,33605,33621,33759,33771,33807,33809,33815,33846,33853,33861,33876,33903,33947,33959,33978,33991,34010,34019,34035,34038,34047,34086,34089,34096,34123,34126,34132,34151,34199,34216,34224,34243,34258,34283,34309,34310,34317,34318,34323,34341,34374,34386,34393,34399,34413,34417,34420,34422,34446,34454,34464,34470,34485,34517,34518,34533,34548,34586,34600,34609,34613,34615,34617,34624,34633,34658,34666,34684,34696,34708,34723,34744,34757,34768,34774,34780,34820,34824,34825,34835,34856,34892,34914,34921,34922,34923,34932,34938,34939,34942,34943,34953,34972,34982,34986,34987,34993,35094,35100,35118,35147,35153,35167,35168,35173,35184,35190,35207,35225,35229,35280,35329,35330,35359,35445,35482,35483,35484,35515,35645,35663,35684,35709,35734,35765,35788,35797,35817,35834,35866,35877,35888,35948,36027,36046,36072,36074,36093,36189,36192,36205,36214,36237,36253,36290,36314,36318,36324,36352,36376,36381,36396,36400,36404,36425,36459,36470,36505,36518,36551,36563,36576,36596,36599,36600,36609,36622,36631,36634,36646,36653,36668,36686,36689,36725,36754,36766,36772,36776,36781,36792,36797,36819,36829,36837,36865,36889,36900,36905,36908,36912,36916,36945,36946,36948,36950,36967,36978,36985,37001,37011,38050,38077,38087,38091,38105,38121,38145,38180,38181,38194,38213,38214,38234,38236,38240,38258,38269,38276,38293,38305,38336,38367,38390,38391,38416,38444,38462,38471,38474,38492,38573,38592,38596,38600,38603,38608,38609,38613,38614,38637,38646,38651,38669,38674,38702,38710,38711,38722,38724,38726,38731,38732,38741,38742,38744,38749,38759,38777,38779,38789,38790,38799,38812,38813,38818,38823,38828,38830,38833,38848,38854,38863,38869,38873,38874,38875,38876,40051,40092,40099,40114,40116,40125,40144,40150,40180,40189,40217,40238,40280,40287,40310,40320,40325,40344,40352,40365,40369,40383,40402,40404,40410,40422,40448,40454,40463,40518,40521,40523,40537,40553,40564,40572,40576,40587,40588,40602,40619,40625,40633,40642,40649,40650,40652,40654,40655,40656,40657,40658,40660,40662,42001,42002,42033,42043,42060,42080,42104,42146,42226,42277,42301,42304,42312,42317,42329,42386,42413,42503,42580,42590,42652,42660,42677,42690,42703,42748,42777,42797,42815,42822,42843,42872,42878,42885,42922,42927,42928,42939,42945,42950,42971,42973,42981,43006,43007,43019,43035,43043,43057,43058,43068,43076,43080,43094,43133,43150,43166,43182,43183,43198,43199,43216,43217,43224,43246,43254,43263,43269,43270,43294,43300,43307,43312,43318,43330,43339,43342,43362,43367,43376,43386,43392,43396,43399,43401,43404,43405,43406,43409,43410,43415,43418,43425,43435,43439,43446,43451,43452,43466,43468,43472,43478,43494,43499,43510,43518,43520,43524,43538,43542,43547,43548,43552,43561,43565,43566,43567,43571,43584,43594,43598,43608,43609,43610,43614,43617,43623,43624,43627,43632,43638,43643,43644,43650,43652,43653,43655,43656,43658,43660,43661,43663,43665,43668,43669,43673,43674,43675,43677,43678,43679,43680,43681,43684,43685,43686,43687,43688,43689,43691,43692,43693,44036,44060,44076,44102,44118,44151,44154,44160,44165,44168,44172,44174,44175,44183,44194,44203,44218,44221,44227,44230,44242,44248,44250,44258,44270,44309,44328,44330,44346,44365,44381,44399,44445,44454,44467,44468,44482,44489,44490,44497,44508,44514,44515,44531,44564,44575,44585,44587,44592,44594,44596,44617,44633,44645,44658,44659,44670,44675,44676,44678,44684,44700,44703,44719,44722,44736,44748,44758,44766,44776,44783,44790,44797,44803,44816,44822,44831,44845,44870,44873,44884,44885,44924,44939,44992,45068,45078,45080,45096,45145,45158,45164,45195,45197,45205,45248,45268,45302,45306,45358,45372,45374,45376,45401,45405,45420,45448,45467,45470,45476,45481,45483,45509,45523,45557,45583,45592,45645,45679,45682,45720,45824,45851,45894,45947,46012,46013,46036,46069,46117,46123,46164,46175,46176,46217,46255,46299,46308,46350,46351,46360,46385,46418,46431,46436,46451,46460,46462,46475,46481,46500,46513,46536,46570,46573,46608,46612,46643,46696,46700,46706,46708,46713,46727,46730,46746,46752,46784,46813,46816,46833,46857,46911,46928,46929,46956,46971,46978,46983,46986,47021,47027,47034,47037,47048,47094,47101,47138,47154,47161,47183,47190,47195,47225,47226,47274,47294,47362,47385,47405,47409,47515,47570,47592,47601,47608,47636,47644,47658,47674,47706,47792,47793,47819,47859,47860,47867,47881,47903,47909,47910,47911,47912,47913,47914,47915,47916,47917,47918,47922,47926,47931,47932,47938,47944,47948,47995,48001,48004,48024,48025,48029,48041,48076,48107,48112,48149,48169,48172,48240,48304,48321,48369,48381,48385,48502,48536,48583,48620,48660,48763,48796,48831,48834,48856,48857,48872,48894,48904,48912,48955,49009,49014,49033,49050,49065,49107,49112,49120,49175,49207,49210,49212,49216,49219,49233,49249,49251,49262,49273,49317,49346,49361,49399,49432,49444,49447,49456,49464,49484,49487,49490,49520,49534,49554,49559,49569,49594,49600,49614,49622,49623,49629,49651,49658,49681,49711,49714,49723,49812,49817,49821,49848,49874,49911,49926,49933,49945,49974,49977,49978,49983,49985,49994,50002,50011,50012,50034,50035,50039,50041,50042,50050,50052,50060,50077,50084,50089,50117,50118,50121,50143,50158,50171,50192,50201,50204,50277,50283,50288,50291,50292,50320,50334,50349,50358,50364,50386,50387,50400,50408,50416,50429,50441,50444,50447,50456,50495,50502,50550,50562,50572,50575,50578,50594,50602,50614,50640,50647,50675,50691,50715,50716,50720,50726,50740,50742,50761,50765,50800,50801,50813,50821,50826,50846,50850,50869,50887,50903,50904,50905,50909,50930,50934,50935,50949,50979,50986,51007,51048,51068,51099,51112,51115,51120,51141,51157,51193,51195,51218,51234,51239,51246,51338,51353,51356,51366,51515,51543,51557,51570,51577,51608,51633,51718,51728,51732,51739,51741,51786,51800,51801,51802,51835,51836,51855,51878,51892,51898,51900,51906,51919,51930,51932,51938,51954,51982,51994,52038,52059,52083,52086,52088,52089,52114,52134,52169,52205,52239,52243,52245,52251,52253,52256,52283,52294,52312,52313,52318,52326,52327,52330,52331,52340,52341,52386,52394,52400,52403,52406,52407,52410,52411,52417,52418,52427,52436,52438,52454,52462,52463,52469,52484,52486,52487,52490,52492,52493,52496,52503,52507,52508,52510,52511,52512,52513,52514,52515,52516,52517,52518,52519,52520,52523,52525,52527,53012,53014,53112,53125,53160,53176,53202,53241,53244,53248,53253,53256,53279,53288,53293,53315,53342,53346,53348,53356,53386,53389,53391,53405,55013,55108,55139,55141,55148,55174,55208,55217,55246,55250,55266,55328,55393,55471,55490,55499,55563,55594,55608,55628,55629,55701,55710,55714,55717,55720,55735,55741,55748,55768,55792,55802,55808,55851,55885,55889,55896,55914,55921,55934,55935,55936,55940,55942,55950,55960,55961,55975,55980,55992,55995,56009,56011,56024,56027,56031,56036,56043,56046,56047,56049,56051,56052,56053,56054,56055,56056,56057,56058,56059,56060,56061,56062,56063,56064,56065,56066,56067,56068,56069,56070,56071,56072,56073,56074,56075,56076,56077,56078,56079,56080,56082,56083,56084,56086,56091,56093,56096,57004,57006,57038,57046,57104,57106,57116,57117,57150,57166,57203,57210,57213,57249,57255,57273,57313,57316,57329,57331,57332,57333,57334,57337,57344,57364,57365,57374,57378,57379,57380,57381,57398,57427,57443,57490,57493,57495,57564,57571,57607,57610,57635,57655,57741,57812,57855,57873,57888,57909,57929,58004,58032,58043,58050,58065,58084,58103,58113,58136,58158,58163,58221,58222,58241,58254,58266,58280,58281,58285,58304,58327,58379,58388,58396,58413,58454,58476,58501,58503,58515,58518,58525,58566,58572,58635,58638,58667,58671,58720,59027,59044,59081,59083,59096,59098,59115,59117,59122,59150,59169,59170,59174,59178,59188,59210,59213,59232,59237,59249,59254,59262,59277,59280,59309,59335,59342,59345,59355,59359,59360,59364,59366,59388,59403,59404,59409,59420,59421,59426,59429,59437,59467,59472,59474,59478,59479,59491,59495,59515,59532,59543,59547,59564,59566,59567,59630,59633,59661,59686,59713,59716,59719,59722,59725,59726,59767,59770,59771,59787,59798,59808,59846,59854,59866,59867,59868,59883,59888,59889,59899,59914,59917,59919,59948,59949,59951,59954,59957,59973,59978,59979,59984,59987,59988,59997,59999,60000,60080,60139,60169,60172,60190,60201,60234,60236,60258,60270,60278,60343,60351,60357,60361,60420,60448,60456,60573,60574,60596,60599,60601,60607,60608,60609,60612,60614,60623,60625,60629,60630,60632,60633,60638,60644,60653,60672,60687,60690,60694,60697,60701,60702,60711,60720,60736,60737,60742,60744,60747,60751,61013,61024,61029,61048,61095,61098,61100,61114,61117,61128,61148,61150,61166,61175,61184,61257,61260,61283,61295,61307,61317,61332,61354,61362,61373,61394,61399,61418,61419,61439,61448,61457,61483,61485,61493,61498,61499,61500,61506,61514,61530,61535,61541,61566,61608,61609,61624,61625,61638,61664,61677,61685,61691,61692,61755,61778,61794,61801,61814,61824,61836,61847,61850,61857,61859,61860,61867,61876,61901,61902,61917,61924,61942,61956,62023,62034,62045,62065,62073,62074,62078,62098,62102,62107,62127,62139,62141,62162,62172,62176,62178,62198,62202,62206,62216,62227,62250,62263,62269,62276,62278,62282,62293,62300,62304,62305,62309,62320,62322,62325,62335,62337,62359,62374,62378,62383,62393,62394,62395,62396,62398,62406,62413,62414,62415,62416,62417,62419,62421,62422,62423,62424,62425,62427,62428,62429,62430,62431,62432,62433,62435,62436)" +
            //"and kdn_kontonr = 42853 " +
            //"and kdn_kontonr in (10020,	10031,	10048,	10065,	10080,	10083,	10093,	11001,	11007,	11106,	11132,	11232,	11243,	11246,	11274,	11281,	11332,	11363,	11415,	11429,	11447,	11452,	11459,	11465,	11467,	11485,	11523,	11527,	11563,	11601,	11692,	11697,	11731,	11783,	11820,	11860,	11927,	11929,	11930,	11938,	11939,	11960,	11965,	11990,	11993,	12016,	12090,	12098,	12143,	12169,	12174,	12179,	12180,	12185,	12186,	12195,	12201,	12225,	12236,	12264,	12281,	12298,	12304,	12316,	12382,	12388,	12396,	12404,	12405,	12441,	12451,	12510,	12526,	12531,	12537,	12564,	12565,	12569,	12573,	12576,	12595,	12604,	12607,	12618,	12638,	12640,	12654,	12684,	12726,	12739,	12752,	12775,	12802,	12805,	12807,	12814,	12832,	12849,	12861,	12880,	12895,	12898,	12926,	12980,	12990,	12991,	12995,	12998,	13051,	13064,	13069,	13073,	13078,	13087,	13090,	13092,	13094,	13095,	13113,	13178,	13190,	13191,	13194,	13210,	13215,	13276,	13309,	13335,	13364,	13380,	13384,	13386,	13402,	13403,	13435,	13472,	13554,	13559,	13572,	13579,	13586,	13594,	13606,	13620,	13629,	13787,	13817,	13824,	13835,	13849,	13853,	13932,	13933,	13938,	14023,	14079,	14090,	14116,	14137,	14184,	14263,	14274,	14396,	14422,	14431,	14432,	14487,	14566,	14626,	14641,	14690,	14715,	14731,	14783,	14786,	14787,	14801,	14823,	14827,	14837,	14844,	14867,	14887,	14917,	14933,	14969,	15049,	15084,	15115,	15251,	15272,	15287,	15362,	15400,	15420,	15430,	15453,	15460,	15463,	15475,	15521,	15527,	15579,	15587,	15596,	15603,	15638,	15685,	15687,	15693,	15699,	15700,	15717,	15733,	15756,	15763,	15781,	15784,	15789,	15811,	15820,	15823,	15836,	15851,	15859,	15895,	15902,	15906,	15916,	15921,	15931,	15957,	15960,	15979,	15982,	15983,	15991,	16000,	16001,	16005,	16012,	16016,	16019,	16021,	16025,	16028,	16029,	16030,	16043,	16046,	16047,	16048,	16049,	16050,	16051,	16052,	16053,	16054,	16055,	16056,	16057,	16058,	16059,	16060,	16062,	16063,	16064,	16065,	16066,	16067,	16068,	16069,	16070,	16071,	16072,	16073,	16074,	16075,	16076,	16077,	16078,	16079,	16080,	16081,	16082,	16083,	16084,	16085,	16086,	16087,	16088,	16089,	16090,	16091,	16092,	16093,	16094,	16095,	16097,	16098,	16099,	16101,	16102,	16103,	16104,	16105,	16108,	16109,	16110,	16111,	16119,	17007,	17013,	17015,	17055,	17060,	17092,	17093,	17161,	17170,	17178,	17195,	17219,	17232,	17246,	17271,	17364,	17388,	17398,	17407,	17415,	17422,	17431,	17444,	17489,	17533,	17544,	17555,	17563,	17615,	17726,	17798,	17818,	17836,	17884,	17897,	17899,	17981,	17982,	17992,	18014,	18034,	18036,	18048,	18094,	18117,	18144,	18160,	18168,	18196,	18197,	18257,	18283,	18287,	18317,	18346,	18372,	18373,	18383,	18389,	18438,	18456,	18474,	18500,	18501,	18517,	18523,	18534,	18554,	18581,	18587,	18590,	18591,	18596,	18608,	18609,	18612,	18615,	18621,	18624,	18625,	18626,	18627,	18628,	18630,	18631,	18632,	18634,	19035,	19049,	19051,	19058,	19059,	19064,	19082,	19088,	19089,	19097,	19105,	19129,	19150,	19174,	19186,	19198,	19202,	19209,	19219,	19243,	19245,	19301,	19392,	19409,	19433,	19437,	19454,	19516,	19569,	19595,	19613,	19646,	19677,	19693,	19721,	19739,	19744,	19752,	19779,	19783,	19791,	19793,	19816,	19822,	19837,	19851,	19854,	19860,	19902,	19909,	19910,	19927,	19946,	19947,	19953,	19957,	19966,	19983,	19995,	20005,	20006,	20015,	20021,	20030,	20036,	20041,	20059,	20079,	20108,	20111,	20112,	20158,	20161,	20196,	20225,	20233,	20239,	20240,	20255,	20284,	20287,	20296,	20302,	20319,	20325,	20334,	20367,	20387,	20388,	20399,	20403,	20424,	20445,	20453,	20473,	20475,	20485,	20492,	20493,	20494,	20524,	20561,	20562,	20563,	20570,	20579,	20597,	20624,	20629,	20633,	20640,	20660,	20664,	20666,	20667,	20670,	20673,	20681,	20685,	20711,	20718,	20721,	20723,	20724,	20727,	20728,	20730,	20731,	20732,	20733,	20734,	21030,	21081,	21118,	21151,	21159,	21210,	21220,	21228,	21248,	21259,	21309,	21337,	21341,	21370,	21443,	21447,	21462,	21554,	21611,	21612,	21641,	21658,	21668,	21691,	21722,	21755,	21831,	21859,	21875,	21878,	21896,	21901,	21902,	21910,	21948,	21950,	21986,	22009,	22010,	22012,	22023,	22025,	22026,	22035,	22052,	22060,	22073,	22089,	22093,	22105,	22128,	22130,	22169,	22189,	22226,	22230,	22252,	22256,	22289,	22303,	22307,	22353,	22356,	22375,	22405,	22426,	22430,	22439,	22444,	22445,	22446,	22449,	22450,	22451,	22452,	22458,	22459,	22470,	22471,	22472,	22473,	22474,	22475,	22476,	22477,	22478,	22479,	22481,	23015,	23053,	23153,	23163,	23165,	23174,	23186,	23206,	23221,	23245,	23255,	23284,	23310,	23317,	23318,	23396,	23398,	23422,	23471,	23513,	23531,	23534,	23600,	23602,	23628,	23660,	23663,	23689,	23699,	23724,	23748,	23924,	23955,	23959,	23963,	23998,	24040,	24050,	24051,	24087,	24090,	24109,	24114,	24118,	24130,	24142,	24151,	24162,	24235,	24271,	24280,	24292,	24305,	24309,	24344,	24365,	24366,	24387,	24390,	24391,	24405,	24423,	24444,	24484,	24493,	24495,	24511,	24514,	24519,	24544,	24561,	24566,	24589,	24596,	24633,	24655,	24670,	24672,	24676,	24682,	24683,	24703,	24709,	24720,	24751,	24776,	24780,	24790,	24799,	24802,	24811,	24818,	24846,	24854,	24867,	24871,	24875,	24876,	24883,	24886,	24887,	24898,	24899,	24902,	24906,	24908,	24910,	24911,	24912,	24914,	24915,	24916,	24917,	24918,	24919,	24920,	24921,	24922,	25012,	25013,	25032,	25035,	25066,	25069,	25156,	25177,	25181,	25203,	25215,	25216,	25230,	25270,	25343,	25358,	25363,	25412,	25516,	25526,	25528,	25531,	25563,	25571,	25576,	25603,	25613,	25622,	25670,	25676,	25682,	25700,	25726,	25759,	25794,	25815,	25849,	25866,	25958,	25966,	25975,	26138,	26165,	26181,	26184,	26187,	26212,	26223,	26237,	26247,	26259,	26267,	26345,	26359,	26377,	26391,	26392,	26403,	26441,	26449,	26450,	26471,	26479,	26508,	26525,	26536,	26554,	26557,	26577,	26585,	26590,	26593,	26605,	26640,	26642,	26649,	26699,	26704,	26709,	26716,	26776,	26778,	26800,	26812,	26832,	26877,	26881,	26902,	26916,	26929,	26990,	27013,	27046,	27055,	27058,	27093,	27095,	27096,	27101,	27161,	27249,	27255,	27263,	27291,	27292,	27296,	27304,	27318,	27338,	27351,	27352,	27357,	27364,	27372,	27381,	27403,	27410,	27455,	27456,	27463,	27507,	27511,	27556,	27568,	27572,	27593,	27599,	27630,	27664,	27673,	27689,	27716,	27725,	27735,	27742,	27747,	27750,	27794,	27796,	27805,	27819,	27825,	27832,	27840,	27867,	27874,	27925,	27965,	27971,	27975,	27979,	28014,	28044,	28106,	28109,	28130,	28152,	28216,	28294,	28297,	28317,	28325,	28408,	28420,	28443,	28464,	28469,	28495,	28507,	28521,	28541,	28555,	28556,	28564,	28587,	28621,	28634,	28635,	28658,	28660,	28661,	28671,	28676,	28694,	28695,	28704,	28708,	28710,	28712,	28714,	29007,	29012,	29032,	29041,	29064,	29065,	29066,	29068,	29069,	29070,	29076,	29077,	29079,	29084,	29098,	29102,	29137,	29151,	29180,	29182,	29191,	29209,	29213,	29215,	29232,	29240,	29244,	29246,	29254,	29277,	29283,	29292,	29298,	29301,	29335,	29339,	29388,	29395,	29405,	29406,	29414,	29437,	29467,	29474,	29481,	29500,	29513,	29515,	29537,	29551,	29563,	29564,	29566,	30009,	30017,	30022,	30023,	30025,	30029,	30084,	30157,	30176,	30184,	30186,	30193,	30207,	30209,	30257,	30307,	30308,	30318,	30324,	30367,	30397,	30417,	30438,	30467,	30473,	30478,	30483,	30497,	30503,	30580,	30594,	30595,	30598,	30635,	30655,	30668,	30684,	30686,	30687,	30701,	30710,	30711,	30725,	30735,	30739,	30744,	30757,	30761,	30775,	30811,	30918,	30922,	30929,	30950,	30954,	30959,	30967,	31015,	31024,	31027,	31048,	31050,	31059,	31090,	31094,	31124,	31155,	31164,	31170,	31171,	31179,	31204,	31212,	31246,	31260,	31268,	31270,	31328,	31344,	31354,	31364,	31374,	31385,	31401,	31421,	31428,	31434,	31462,	31464,	31477,	31483,	31508,	31540,	31546,	31553,	31560,	31627,	31645,	31655,	31659,	31664,	31689,	31691,	31726,	31741,	31753,	31824,	31831,	31859,	31860,	31871,	31879,	31900,	31912,	31932,	31952,	31961,	31983,	31985,	31994,	32031,	32034,	32046,	32058,	32061,	32063,	32104,	32106,	32113,	32118,	32134,	32138,	32140,	32167,	32169,	32185,	32186,	32188,	32190,	32193,	32194,	32217,	32226,	32242,	32243,	32250,	32254,	32256,	32271,	32275,	32301,	32319,	32329,	32336,	32391,	32407,	32437,	32449,	32471,	32478,	32486,	32487,	32490,	32492,	32517,	32528,	32533,	32548,	32552,	32574,	32595,	32599,	32605,	32609,	32645,	32649,	32653,	32654,	32661,	32666,	32684,	32696,	32701,	32703,	32706,	32718,	32719,	32722,	32724,	32727,	32728,	32731,	32733,	32736,	32742,	32744,	32751,	32759,	32760,	32762,	32764,	32770,	32772,	32774,	32776,	32777,	32778,	32779,	32780,	32781,	32782,	32783,	32784,	32786,	33003,	33032,	33080,	33105,	33145,	33171,	33188,	33208,	33210,	33236,	33244,	33265,	33314,	33316,	33343,	33407,	33424,	33455,	33459,	33463,	33486,	33528,	33532,	33564,	33605,	33621,	33759,	33771,	33807,	33809,	33815,	33846,	33853,	33861,	33876,	33947,	33959,	33978,	33991,	34010,	34019,	34035,	34038,	34086,	34089,	34096,	34123,	34132,	34151,	34199,	34216,	34224,	34243,	34309,	34318,	34323,	34341,	34374,	34393,	34399,	34413,	34417,	34420,	34422,	34446,	34454,	34464,	34470,	34485,	34517,	34518,	34533,	34586,	34600,	34609,	34613,	34615,	34617,	34624,	34633,	34658,	34666,	34684,	34696,	34708,	34723,	34744,	34757,	34768,	34774,	34820,	34824,	34825,	34835,	34856,	34892,	34921,	34922,	34923,	34932,	34938,	34939,	34943,	34953,	34972,	34982,	34986,	34987,	34993,	35094,	35100,	35118,	35147,	35153,	35167,	35168,	35173,	35184,	35190,	35225,	35229,	35280,	35329,	35330,	35445,	35482,	35483,	35484,	35515,	35645,	35663,	35684,	35709,	35734,	35765,	35788,	35797,	35817,	35834,	35866,	35877,	35888,	35948,	36027,	36046,	36072,	36074,	36093,	36189,	36192,	36205,	36214,	36237,	36253,	36290,	36318,	36324,	36352,	36376,	36381,	36396,	36400,	36404,	36425,	36459,	36470,	36505,	36518,	36551,	36576,	36596,	36599,	36600,	36622,	36631,	36634,	36646,	36653,	36668,	36686,	36689,	36725,	36754,	36766,	36772,	36776,	36781,	36792,	36819,	36829,	36837,	36865,	36889,	36900,	36905,	36912,	36916,	36945,	36946,	36948,	36950,	36967,	36978,	36985,	37001,	37011,	38050,	38077,	38087,	38091,	38105,	38121,	38145,	38180,	38181,	38194,	38213,	38214,	38234,	38236,	38240,	38258,	38269,	38276,	38293,	38305,	38336,	38367,	38390,	38391,	38416,	38444,	38462,	38471,	38474,	38492,	38573,	38592,	38600,	38603,	38609,	38613,	38637,	38669,	38674,	38710,	38711,	38722,	38724,	38726,	38732,	38741,	38742,	38744,	38749,	38777,	38779,	38789,	38790,	38812,	38813,	38818,	38823,	38828,	38830,	38833,	38848,	38854,	38863,	38869,	38873,	38874,	38875,	40051,	40092,	40099,	40114,	40116,	40125,	40144,	40150,	40180,	40189,	40217,	40280,	40310,	40320,	40325,	40344,	40352,	40365,	40369,	40383,	40402,	40404,	40410,	40422,	40448,	40454,	40463,	40518,	40521,	40523,	40537,	40553,	40564,	40572,	40576,	40587,	40588,	40602,	40619,	40625,	40633,	40642,	40649,	40650,	40652,	40654,	40655,	40656,	40657,	40658,	40660,	40662,	42001,	42002,	42033,	42043,	42060,	42080,	42104,	42146,	42226,	42277,	42301,	42304,	42329,	42386,	42413,	42503,	42580,	42590,	42652,	42660,	42690,	42703,	42748,	42777,	42815,	42822,	42843,	42872,	42878,	42922,	42927,	42928,	42939,	42945,	42950,	42971,	42973,	42981,	43019,	43035,	43043,	43057,	43058,	43068,	43076,	43080,	43094,	43133,	43150,	43166,	43182,	43183,	43198,	43199,	43216,	43217,	43224,	43246,	43254,	43263,	43269,	43270,	43294,	43300,	43307,	43312,	43318,	43330,	43339,	43342,	43362,	43367,	43376,	43386,	43392,	43396,	43399,	43401,	43404,	43405,	43406,	43410,	43415,	43418,	43425,	43435,	43439,	43446,	43452,	43466,	43468,	43472,	43478,	43494,	43510,	43518,	43520,	43524,	43538,	43542,	43547,	43548,	43552,	43565,	43566,	43567,	43571,	43584,	43594,	43598,	43608,	43609,	43610,	43614,	43617,	43623,	43624,	43627,	43632,	43638,	43643,	43644,	43650,	43652,	43653,	43655,	43656,	43658,	43660,	43661,	43663,	43665,	43668,	43669,	43673,	43674,	43675,	43677,	43678,	43679,	43680,	43681,	43684,	43685,	43687,	43688,	43689,	43692,	43693,	44036,	44060,	44076,	44102,	44118,	44160,	44165,	44168,	44172,	44174,	44175,	44183,	44194,	44203,	44218,	44221,	44227,	44230,	44242,	44248,	44250,	44258,	44309,	44328,	44330,	44346,	44365,	44381,	44399,	44445,	44454,	44468,	44489,	44490,	44515,	44531,	44564,	44575,	44585,	44587,	44592,	44594,	44596,	44617,	44633,	44645,	44659,	44675,	44676,	44678,	44684,	44700,	44703,	44719,	44736,	44748,	44758,	44766,	44776,	44783,	44790,	44797,	44816,	44822,	44845,	44870,	44873,	44884,	44885,	44924,	44939,	44992,	45068,	45078,	45080,	45096,	45145,	45158,	45195,	45197,	45205,	45248,	45268,	45302,	45306,	45358,	45372,	45374,	45376,	45405,	45420,	45448,	45467,	45470,	45476,	45481,	45483,	45509,	45523,	45557,	45583,	45592,	45645,	45679,	45682,	45720,	45824,	45851,	45947,	46012,	46013,	46036,	46069,	46117,	46123,	46164,	46175,	46217,	46255,	46299,	46308,	46350,	46351,	46360,	46385,	46431,	46436,	46451,	46460,	46462,	46475,	46481,	46500,	46513,	46536,	46570,	46573,	46608,	46612,	46643,	46696,	46700,	46706,	46708,	46713,	46730,	46746,	46752,	46784,	46813,	46816,	46833,	46857,	46911,	46929,	46971,	46978,	46983,	46986,	47021,	47027,	47034,	47037,	47048,	47094,	47101,	47138,	47154,	47161,	47183,	47190,	47195,	47225,	47274,	47294,	47362,	47385,	47405,	47409,	47515,	47570,	47601,	47636,	47644,	47658,	47674,	47706,	47792,	47793,	47819,	47859,	47860,	47881,	47903,	47909,	47910,	47911,	47912,	47913,	47914,	47915,	47916,	47917,	47918,	47922,	47926,	47931,	47932,	47938,	47944,	47948,	47995,	48001,	48004,	48024,	48025,	48029,	48041,	48076,	48107,	48112,	48149,	48169,	48240,	48304,	48321,	48369,	48381,	48502,	48536,	48583,	48620,	48660,	48763,	48796,	48831,	48834,	48856,	48857,	48872,	48894,	48904,	48912,	48955,	49014,	49033,	49050,	49065,	49107,	49112,	49120,	49175,	49207,	49210,	49212,	49216,	49219,	49233,	49249,	49251,	49262,	49273,	49317,	49346,	49361,	49399,	49432,	49444,	49447,	49456,	49464,	49484,	49487,	49520,	49534,	49554,	49559,	49569,	49594,	49600,	49614,	49622,	49623,	49629,	49651,	49714,	49723,	49812,	49817,	49821,	49848,	49874,	49926,	49933,	49945,	49974,	49977,	49978,	49983,	49985,	49994,	50002,	50011,	50012,	50034,	50035,	50039,	50042,	50050,	50060,	50077,	50089,	50117,	50118,	50121,	50143,	50158,	50171,	50192,	50201,	50204,	50277,	50283,	50288,	50291,	50292,	50320,	50334,	50349,	50358,	50364,	50386,	50387,	50400,	50408,	50416,	50441,	50444,	50447,	50456,	50495,	50502,	50550,	50562,	50572,	50575,	50578,	50594,	50614,	50640,	50647,	50675,	50691,	50715,	50720,	50726,	50740,	50742,	50761,	50765,	50800,	50801,	50813,	50821,	50826,	50846,	50869,	50887,	50903,	50904,	50905,	50909,	50930,	50934,	50935,	50949,	50979,	51007,	51048,	51099,	51112,	51115,	51120,	51141,	51157,	51193,	51195,	51218,	51234,	51239,	51246,	51338,	51353,	51356,	51366,	51515,	51543,	51557,	51570,	51577,	51608,	51633,	51718,	51728,	51732,	51739,	51741,	51786,	51800,	51801,	51802,	51835,	51836,	51855,	51878,	51892,	51898,	51900,	51906,	51919,	51930,	51932,	51938,	51954,	51982,	51994,	52059,	52083,	52086,	52088,	52089,	52114,	52169,	52205,	52239,	52243,	52245,	52251,	52256,	52283,	52294,	52312,	52313,	52318,	52326,	52327,	52330,	52331,	52340,	52341,	52386,	52394,	52400,	52406,	52407,	52410,	52411,	52417,	52418,	52436,	52438,	52454,	52462,	52463,	52469,	52484,	52486,	52487,	52490,	52492,	52493,	52496,	52503,	52507,	52508,	52510,	52511,	52512,	52513,	52514,	52515,	52517,	52518,	52519,	53012,	53014,	53112,	53125,	53160,	53202,	53241,	53244,	53248,	53253,	53256,	53279,	53288,	53293,	53315,	53342,	53346,	53348,	53356,	53386,	53389,	53405,	55013,	55108,	55139,	55141,	55148,	55174,	55208,	55217,	55246,	55250,	55266,	55328,	55393,	55471,	55608,	55628,	55629,	55701,	55710,	55714,	55717,	55720,	55735,	55741,	55748,	55768,	55792,	55808,	55851,	55885,	55889,	55896,	55914,	55921,	55934,	55935,	55936,	55940,	55942,	55950,	55960,	55961,	55975,	55980,	55992,	55995,	56009,	56011,	56024,	56027,	56031,	56036,	56043,	56046,	56047,	56049,	56051,	56052,	56053,	56054,	56055,	56056,	56057,	56058,	56059,	56060,	56061,	56062,	56063,	56064,	56065,	56066,	56067,	56068,	56069,	56070,	56071,	56072,	56073,	56074,	56075,	56077,	56078,	56082,	56083,	57004,	57006,	57038,	57046,	57104,	57106,	57116,	57117,	57166,	57203,	57210,	57249,	57255,	57273,	57313,	57316,	57329,	57331,	57332,	57333,	57334,	57337,	57344,	57364,	57365,	57374,	57378,	57379,	57380,	57381,	57427,	57443,	57490,	57493,	57495,	57564,	57571,	57607,	57610,	57635,	57655,	57741,	57812,	57855,	57873,	57909,	57929,	58004,	58032,	58043,	58050,	58065,	58103,	58113,	58136,	58158,	58221,	58222,	58241,	58254,	58266,	58280,	58281,	58285,	58304,	58327,	58379,	58388,	58413,	58454,	58476,	58501,	58503,	58515,	58518,	58525,	58566,	58572,	58635,	58638,	58667,	58671,	58720,	59027,	59044,	59081,	59083,	59096,	59098,	59115,	59117,	59122,	59150,	59169,	59174,	59178,	59188,	59210,	59213,	59232,	59237,	59249,	59254,	59262,	59277,	59309,	59335,	59342,	59345,	59355,	59364,	59366,	59403,	59404,	59409,	59420,	59421,	59429,	59437,	59467,	59472,	59474,	59478,	59479,	59491,	59495,	59515,	59532,	59543,	59547,	59566,	59567,	59633,	59661,	59686,	59713,	59716,	59722,	59725,	59726,	59767,	59770,	59771,	59787,	59798,	59846,	59854,	59866,	59867,	59868,	59883,	59888,	59889,	59899,	59914,	59917,	59919,	59948,	59949,	59951,	59954,	59957,	59973,	59978,	59979,	59984,	59987,	59988,	59997,	60000,	60080,	60139,	60169,	60172,	60190,	60201,	60234,	60236,	60258,	60270,	60278,	60351,	60357,	60361,	60420,	60456,	60573,	60574,	60596,	60599,	60601,	60608,	60609,	60612,	60614,	60623,	60625,	60630,	60632,	60633,	60638,	60644,	60653,	60672,	60687,	60690,	60694,	60697,	60701,	60702,	60711,	60720,	60736,	60737,	60742,	60744,	60751,	61013,	61024,	61029,	61048,	61095,	61098,	61100,	61114,	61148,	61150,	61166,	61175,	61184,	61257,	61260,	61283,	61295,	61307,	61317,	61332,	61354,	61362,	61394,	61418,	61419,	61439,	61448,	61457,	61483,	61485,	61493,	61498,	61499,	61500,	61506,	61514,	61530,	61535,	61541,	61566,	61624,	61625,	61638,	61664,	61677,	61685,	61691,	61692,	61755,	61778,	61794,	61801,	61814,	61824,	61836,	61847,	61850,	61857,	61859,	61860,	61867,	61876,	61901,	61917,	61924,	61956,	62023,	62034,	62045,	62065,	62073,	62074,	62078,	62098,	62102,	62107,	62127,	62139,	62141,	62162,	62172,	62176,	62178,	62198,	62202,	62206,	62216,	62263,	62269,	62276,	62278,	62282,	62293,	62300,	62304,	62309,	62320,	62322,	62325,	62335,	62337,	62359,	62374,	62378,	62383,	62393,	62394,	62395,	62396,	62398,	62406,	62413,	62414,	62415,	62416,	62417,	62419,	62421,	62422,	62423,	62424,	62425,	62427,	62428,	62429,	62430,	62431) " +
            "and his_redat < " + endDate + " " +
            "and his_redat > " + startDate + " " +
            //"and ans_aenderung < " + DateTime.Now.AddDays(1).ToString("yyyyMMdd") + " " +
            //"and ans_aenderung > " + DateTime.Now.AddDays(-4).ToString("yyyyMMdd") + " " +
            //"and ans_aenderung > 20240401 " +
            "group by kdn_kontonr";

            Operations operations = new Operations();
            OdbcCommand adapterKunde = new OdbcCommand(selectCommand2, connection);
            List<DataTable> dataTables = new List<DataTable>();
            dataTable = new DataTable("SDL_Kundenimport");
            dataTable = operations.WriteTable(adapterKunde, dataTable, connection);

            foreach (DataRow rowKd in dataTable.Rows)
            {
                string konto = rowKd[0].ToString();

                try
                {
                    if (Atradius.Rows.Contains(rowKd[0].ToString()))
                    {
                        foreach (DataRow row in Atradius.Rows)

                        {
                            if (row[0].ToString() == rowKd[0].ToString())
                            {
                                rowKd[81] = "216426";
                                rowKd[82] = row[1].ToString().Remove('.');
                                //rowKd[83] = row[4].ToString();                                                                          
                                rowKd[84] = "B";
                                rowKd[85] = row[5];
                            }
                        }
                    }
                    else
                    {
                        rowKd[81] = "";
                        rowKd[82] = "";
                        rowKd[83] = "";
                        rowKd[84] = "";
                        rowKd[85] = "";
                    }

                    if (kobensen.Rows.Contains(rowKd[0].ToString()))
                    {
                        foreach (DataRow row in kobensen.Rows)
                        {
                            if (row[0].ToString() == rowKd[0].ToString())
                            {
                                rowKd[50] = operations.RemoveWhiteSpace(row[2].ToString());
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    WriteLog(e.Message);
                }

                if (Dictionaries.JungheinrichDict.ContainsKey(rowKd[0].ToString()))
                {

                    rowKd[82] = "700000";
                }

                else
                {
                    rowKd[0] = Standort + rowKd[0].ToString();
                }

                rowKd[37] = Operations.SetBrancheNr(rowKd[37].ToString());

                if (rowKd[3].ToString() == "NL")
                {
                    rowKd[13] = operations.SetPLZ(rowKd[13].ToString(), rowKd[4].ToString());
                    rowKd[4] = operations.SetOrtNL(rowKd[4].ToString());
                }

                rowKd[6] = rowKd[4].ToString().ToUpper();

                if (rowKd[17] != null)
                {
                    if (rowKd[3].ToString() != "FR")
                    {
                        string street = rowKd[17].ToString();
                        string hNr = rowKd[19].ToString();
                        try
                        {
                            (street, hNr) = Operations.GetHnr(rowKd[17].ToString());
                            rowKd[17] = street;
                            rowKd[19] = hNr;
                        }
                        catch (Exception e)
                        {
                            WriteLog(e.Message);
                        }
                    }
                    else
                    {
                        rowKd[19] = "@";
                    }
                }

                rowKd[17] = operations.CorrectStrasse(rowKd[17].ToString());

                if (int.Parse(rowKd[46].ToString()) > 0)
                {
                    rowKd[46] = operations.DateConvert(rowKd[46].ToString());
                }
                else
                {
                    rowKd[46] = "@";
                }

                if (rowKd[56].ToString() != "0")
                {

                    if (Dictionaries.VerbandDict.ContainsKey(rowKd[56].ToString()))
                    {
                        rowKd[56] = Dictionaries.VerbandDict[rowKd[56].ToString()];
                    }
                    else
                    {
                        if (Dictionaries.VerbaendeDict.ContainsKey(konto))
                        {
                            rowKd[56] = Dictionaries.VerbaendeDict[konto];
                        }
                        else
                        {
                            rowKd[56] = "";
                        }
                    }
                }
                else
                {
                    rowKd[56] = string.Empty;
                }


                if (rowKd[56].ToString() != "")
                {
                    rowKd[60] = GetNrVerb(rowKd[11].ToString(), rowKd[12].ToString(), rowKd[56].ToString());
                    rowKd[61] = "JA";
                }
                else if (rowKd[56] == "")
                {
                    rowKd[61] = "";
                }

                if (rowKd[53].ToString() == "0")
                {
                    rowKd[53] = string.Empty;
                }
                else if (rowKd[53].ToString() != null | rowKd[53].ToString() != "")
                {
                    rgzahler.Add(rowKd[53].ToString());
                    rowKd[53] = Standort + rowKd[53].ToString();
                }
                else
                {
                    rowKd[53] = "";
                }

                if (rowKd[53].ToString() == "1")
                {
                    rowKd[53] = "";
                }

                if (rowKd[3].ToString().ToUpper() == "DE")
                {
                    try
                    {
                        rowKd[20] = Dictionaries.BundeslandDict[rowKd[13].ToString()];
                        rowKd[17] = operations.CorrectStrasse(rowKd[17].ToString());
                    }

                    catch (KeyNotFoundException knfe)
                    {
                        rowKd[20] = "";
                    }
                }

                else
                {
                    rowKd[20] = "";
                }

                rowKd[38] = operations.GetRegion(rowKd[3].ToString(), rowKd[13].ToString());

                if (Dictionaries.sprachenDict.ContainsKey(rowKd[40].ToString()))
                {
                    rowKd[40] = Dictionaries.sprachenDict[rowKd[40].ToString()];
                }

                else
                {
                    rowKd[40] = "";
                }

                if (rowKd[40].ToString() == "D" | rowKd[3].ToString() == "DE" | rowKd[3].ToString() == "CH" | rowKd[3].ToString() == "AT")
                {
                    rowKd[7] = "Sehr geehrte Damen und Herren";
                }
                else if (rowKd[40].ToString() == "F" | rowKd[3].ToString() == "FR")
                {
                    rowKd[7] = "Mesdames, Messieurs";

                }
                else if (rowKd[40].ToString() == "E")
                {
                    rowKd[7] = "Dear Sir / Madam";
                }
                else if (rowKd[40].ToString() == "DU")
                {
                    rowKd[7] = "Geachte dames en heren";
                }
                else
                {
                    rowKd[7] = rowKd[7];
                }

                rowKd[50] = operations.RemoveWhiteSpace(rowKd[50].ToString());

                if (rowKd[50].ToString() != string.Empty && rowKd[50].ToString().Length > 3)
                {
                    if (!Dictionaries.LandUstidList.Contains(rowKd[50].ToString().Substring(0, 2)) && rowKd[50].ToString().Length < 21)
                    {
                        MatchCollection matches = Regex.Matches(rowKd[50].ToString(), @"\d+");
                        rowKd[48] = matches[0].Value.ToString();
                        rowKd[50] = "";
                    }
                }

                if (rowKd[50].ToString().Length > 20)
                {
                    rowKd[50] = "";
                }

                if (rowKd[35].ToString() == "" | rowKd[35].ToString() == null)
                {
                    rowKd[35] = rowKd[4].ToString().ToUpper(); ;
                }

                if (Standort == 2 && Dictionaries.ZbdNrHBS2SDL.ContainsKey(rowKd[71].ToString()))
                {

                    rowKd[71] = Dictionaries.ZbdNrHBS2SDL[rowKd[71].ToString()];
                }

                else if (Dictionaries.ZbdNrVK.ContainsKey(rowKd[71].ToString()))
                {
                    rowKd[71] = Dictionaries.ZbdNrVK[rowKd[71].ToString()];
                }

                else
                {
                    rowKd[71] = "";
                }

                if (rowKd[93].ToString() != "0")
                {
                    rowKd[93] = "Ja";
                }

                else
                {
                    rowKd[93] = "Nein";
                }

                if (rowKd[52].ToString() == "0")
                {
                    rowKd[52] = "Nein";
                }

                else
                {
                    rowKd[52] = "Ja";
                }

                if (rowKd[73].ToString() == "J")
                {
                    rowKd[73] = "Ja";
                }

                else if (rowKd[73].ToString() == "N")
                {
                    rowKd[73] = "JA";
                }

                if (rowKd[78] != null && Dictionaries.VADict.ContainsKey(rowKd[78].ToString()))
                {
                    rowKd[78] = Dictionaries.VADict[rowKd[78].ToString()];
                }

                else
                {
                    rowKd[78] = "@";
                }

                if (rowKd[87] != null)
                {
                    string preisliste = string.Empty;
                    string rabattgruppe = string.Empty;

                    try
                    {
                        (rabattgruppe, preisliste) = operations.GetKundenrabattgruppe(rowKd[87].ToString());
                    }

                    catch (KeyNotFoundException knfe)
                    {
                        rowKd[87] = "@";
                    }

                    rowKd[87] = rabattgruppe;
                    rowKd[86] = preisliste;
                }

                rowKd[89] = "Ja";
                rowKd[91] = "@";

                if (operations.Betriebskalender.ContainsKey(rowKd[20].ToString()))
                {
                    rowKd[144] = operations.Betriebskalender[rowKd[20].ToString()];
                }

                if (rowKd[0].ToString().Length < 6)
                {
                    rowKd[0] = Standort + rowKd[0].ToString();
                }

                while (rowKd[37].ToString().Length < 3)
                {
                    rowKd[37] = "0" + rowKd[37].ToString();
                }

                if (rowKd[53].ToString() == "1")
                {
                    rowKd[53] = string.Empty;
                }
            }

            return dataTable;
        }

        public DataTable GetKundenRgz(List<string> zahler)
        {
            string concat = "";

            foreach (var z in zahler)
            {
                concat += z + ",";
            }

            concat = concat.Substring(0, concat.Length - 1);
            dataTable = new DataTable("Kundendaten__9_3");
            OdbcConnection connection = new OdbcConnection("DSN=Parity_SDL;Pooling=true;");
            Standort = 1;

            string selectCommand2 = "SELECT " +
                    "kunden.kdn_kontonr," +
                    "'', " +
                    "anschrift.name_001, " +
                    "anschrift.land, " +
                    "anschrift.ort, " +
                    "anschrift.anssuch, " +
                    "anschrift.ans_suwo2, " +
                    "'@', " +
                    "'@'," +
                    "'@', " +                              //09
                    "'@', " +
                    "anschrift.name_002," +                //11
                    "anschrift.name_003, " +
                    "anschrift.plz," +
                    " '@' AS Expr3, " +
                    "anschrift.ans_teilort, " +
                    "'@' AS Expr4, " +
                    "anschrift.strasse, " +
                    "'@' AS Expr5, " +
                    "'Hausnummer' AS Hausnummer," +
                    "'@' AS Bundesland, " +
                    "anschrift.ans_pf_plz, " +
                    "anschrift.ans_postfach, " +
                    "'Ja' AS Expr8, " +
                    "anschrift.ans_email, " +
                    "anschrift.ans_homepage, " +
                    "anschrift.ans_telex, " +
                    "anschrift.ans_telefon, " +
                    "'' as 'Autotelefon'," +             //28
                    "anschrift.ans_telefax, " +
                    "'' as 'Telefon2'," +
                    "'' as 'Telefax2'," +
                    "'' as 'Längengrad'," +
                    "'' as 'Breitengrad'," +
                    "anschrift.anssuch AS Expr10, " +
                    "anschrift.ans_suwo2 AS Expr11, " +
                    "'VK'," +
                    "kunden.kdn_x_branche, " +
                    "'@' AS 'Region'," +               //38
                    "'@', " +
                    "kdn_sprnr, " +                   //40
                    "'' as 'ABC-Klasse'," +
                    "'@' AS 'Teilestatistik', " +
                    "'@' AS 'Webshop', " +
                    "'@' AS 'Bestandsfaktor', " +
                    "'@' AS 'Lagerort', " +
                    "kunden.kdn_erstellt, " +
                    "'' as 'Nummer bei Lieferant', " +            //48
                    "'', " +          //48
                    "'0'," +
                    "anschrift.ans_ustid, " +         //50
                    "'@' AS 'Rechnungsintervall', " +
                    "kunden.kdn_faktkz, " +
                    "'' as 'Rechnungszahler', " +
                    "'@' AS  'Konzern', " +
                    "'@' AS 'Bezeichnung', " +
                    "kunden.kdn_ekvnr, " +                          //56          
                    "(select f1e_x_ekname from f1ekverband where f1ekverband.f1e_x_eknr = kunden.kdn_ekvnr limit 1) AS f1ekname, " +
                    "'@' AS Expr24, " +                             //55
                    "'@' AS Expr25, " +
                    "'@' AS Expr26, " +
                    "'@' AS Expr27, " +
                    "'@' AS Expr28, " +
                    "'1', " +                                       //63
                    "'2', " +
                    "'3', " +
                    "'4', " +
                    "'5', " +
                    "'6', " +
                    "'3' as 'Zahlungsart', " +                      //69
                    "'@' AS Expr36, " +
                    "kdn_zbnr, " +                                  //71    
                    "kunden.kdn_kredlimit, " +                      //72
                    "kdn_c_ohneklm, " +                             //73
                    "'2' AS 'Mahnverfahren', " +                    //74
                    "'@' AS Expr39, " +                             //75
                    "'@' AS Expr40, " +                             //76
                    "'@' AS Expr41, " +                             //77
                    "kunden.kdn_vsanr, " +                          //78
                    "kdn_lbdnr," +                                        //79
                    "'1' as 'Lieferrestriktion',  " +               //80    
                    "'' as 'Vertrags'," +                                          //81
                    "'@' AS 'Risikonummer WKV', " +
                    "'' as 'Kreditversicherung', " +
                    "'@' AS 'WKV-Kennzeichen', " +
                    "'@' as 'l. Auskunft', " +
                    "kunden.kdn_x_adrsel, " +                       //86
                    "kdn_x_adrsel||'_'||anschrift.land||'_'||kdn_vertrnr_001, " +//87
                    "kdn_x_rabattausw, " +                         //88
                    "'Ja' AS Expr50, " +                           //89
                    "'Ja' AS Expr51, " +                           //90
                    "'@' AS Expr52, " +                            //91 
                    "'0'as 'Sammellieferschein', " +               //92 
                    "kdn_x_ust_wann," +                            //93
                    "'' as 'Bankverbindung'," +
                    "'Ja' as 'Default'," +
                    "'@' ," +
                    "'1' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "kdn_rekopie," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' as 'Versandkalender'," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@' ," +
                    "'@', " +
                    "'@'," +
                    "'@' " +           //146
                    "FROM kunden, anschrift " +
                    "WHERE (anschrift.ansnr = kunden.kdn_lfdnr) " +
                    "and kdn_typ = 'D' " +
                    "and his_redat > " + DateTime.Now.AddDays(-2).ToString("yyyyMMdd") + " " +
                    "and his_redat < " + DateTime.Now.AddDays(1).ToString("yyyyMMdd") + " " +
                    "group by kdn_kontonr";

            Operations operations = new Operations();
            OdbcCommand adapterKunde = new OdbcCommand(selectCommand2, connection);
            List<DataTable> dataTables = new List<DataTable>();
            dataTable = new DataTable("SDL_Kundenimport");
            dataTable = operations.WriteTable(adapterKunde, dataTable, connection);

            foreach (DataRow rowKd in dataTable.Rows)
            {
                string konto = rowKd[0].ToString();

                try
                {
                    if (Atradius.Rows.Contains(rowKd[0].ToString()))
                    {
                        foreach (DataRow row in Atradius.Rows)

                        {
                            if (row[0].ToString() == rowKd[0].ToString())
                            {
                                rowKd[81] = "216426";
                                rowKd[82] = row[1].ToString().Remove('.');
                                //rowKd[83] = row[4].ToString();                                                                          
                                rowKd[84] = "B";
                                rowKd[85] = row[5];
                            }
                        }
                    }
                    else
                    {
                        rowKd[81] = "";
                        rowKd[82] = "";
                        rowKd[83] = "";
                        rowKd[84] = "";
                        rowKd[85] = "";
                    }

                    if (kobensen.Rows.Contains(rowKd[0].ToString()))
                    {
                        foreach (DataRow row in kobensen.Rows)
                        {
                            if (row[0].ToString() == rowKd[0].ToString())
                            {
                                rowKd[50] = operations.RemoveWhiteSpace(row[2].ToString());
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    WriteLog(e.Message);
                }

                if (Dictionaries.JungheinrichDict.ContainsKey(rowKd[0].ToString()))
                {
                    rowKd[0] = Dictionaries.JungheinrichDict[rowKd[0].ToString()];
                    rowKd[82] = "700000";
                }

                else
                {
                    rowKd[0] = Standort + rowKd[0].ToString();
                }

                rowKd[37] = Operations.SetBrancheNr(rowKd[37].ToString());

                if (rowKd[3].ToString() == "NL")
                {
                    rowKd[13] = operations.SetPLZ(rowKd[13].ToString(), rowKd[4].ToString());
                    rowKd[4] = operations.SetOrtNL(rowKd[4].ToString());
                }

                rowKd[6] = rowKd[4].ToString().ToUpper();

                if (rowKd[17] != null)
                {
                    if (rowKd[3].ToString() != "FR")
                    {
                        string street = rowKd[17].ToString();
                        string hNr = rowKd[19].ToString();
                        try
                        {
                            (street, hNr) = Operations.GetHnr(rowKd[17].ToString());
                            rowKd[17] = street;
                            rowKd[19] = hNr;
                        }
                        catch (Exception e)
                        {
                            WriteLog(e.Message);
                        }
                    }
                    else
                    {
                        rowKd[19] = "@";
                    }
                }

                rowKd[17] = operations.CorrectStrasse(rowKd[17].ToString());

                if (int.Parse(rowKd[46].ToString()) > 0)
                {
                    rowKd[46] = operations.DateConvert(rowKd[46].ToString());
                }
                else
                {
                    rowKd[46] = "@";
                }

                if (rowKd[56].ToString() != "0")
                {

                    if (Dictionaries.VerbandDict.ContainsKey(rowKd[56].ToString()))
                    {
                        rowKd[56] = Dictionaries.VerbandDict[rowKd[56].ToString()];
                    }
                    else
                    {
                        if (Dictionaries.VerbaendeDict.ContainsKey(konto))
                        {
                            rowKd[56] = Dictionaries.VerbaendeDict[konto];
                        }
                        else
                        {
                            rowKd[56] = "";
                        }
                    }
                }
                else
                {
                    rowKd[56] = string.Empty;
                }


                if (rowKd[56].ToString() == "119516" | rowKd[56].ToString() == "138145" | rowKd[56].ToString() == "119433" | rowKd[56].ToString() == "119693")
                {
                    rowKd[60] = operations.GetNrVerb(rowKd[11].ToString(), rowKd[12].ToString(), rowKd[56].ToString());
                    rowKd[61] = "JA";
                }
                else
                {
                    rowKd[61] = "";
                }

                if (rowKd[53].ToString() == "0")
                {
                    rowKd[53] = string.Empty;
                }
                else if (rowKd[53].ToString() != null | rowKd[53].ToString() != "")
                {
                    rowKd[53] = Standort + rowKd[53].ToString();
                }
                else
                {
                    rowKd[53] = "";
                }

                if (rowKd[53].ToString() == "1")
                {
                    rowKd[53] = "";
                }

                if (rowKd[3].ToString().ToUpper() == "DE")
                {
                    try
                    {
                        rowKd[20] = Dictionaries.BundeslandDict[rowKd[13].ToString()];
                        rowKd[17] = operations.CorrectStrasse(rowKd[17].ToString());
                    }

                    catch (KeyNotFoundException knfe)
                    {
                        rowKd[20] = "";
                    }
                }

                else
                {
                    rowKd[20] = "";
                }

                rowKd[38] = operations.GetRegion(rowKd[3].ToString(), rowKd[13].ToString());

                if (Dictionaries.sprachenDict.ContainsKey(rowKd[40].ToString()))
                {
                    rowKd[40] = Dictionaries.sprachenDict[rowKd[40].ToString()];
                }

                else
                {
                    rowKd[40] = "";
                }

                if (rowKd[40].ToString() == "D" | rowKd[3].ToString() == "DE" | rowKd[3].ToString() == "CH" | rowKd[3].ToString() == "AT")
                {
                    rowKd[7] = "Sehr geehrte Damen und Herren";
                }
                else if (rowKd[40].ToString() == "F" | rowKd[3].ToString() == "FR")
                {
                    rowKd[7] = "Mesdames, Messieurs";
                    rowKd[40] = "E";
                }
                else if (rowKd[40].ToString() == "E")
                {
                    rowKd[7] = "Dear Sir / Madam";
                }
                else if (rowKd[40].ToString() == "DU")
                {
                    rowKd[7] = "Geachte dames en heren";
                    rowKd[40] = "E";
                }
                else
                {
                    rowKd[7] = rowKd[7];
                }

                if (rowKd[40].ToString() != "D")
                {
                    rowKd[40] = "E";
                }

                rowKd[50] = operations.RemoveWhiteSpace(rowKd[50].ToString());

                if (rowKd[50].ToString() != string.Empty && rowKd[50].ToString().Length > 3)
                {
                    if (!Dictionaries.LandUstidList.Contains(rowKd[50].ToString().Substring(0, 2)) && rowKd[50].ToString().Length < 21)
                    {
                        MatchCollection matches = Regex.Matches(rowKd[50].ToString(), @"\d+");
                        rowKd[48] = matches[0].Value.ToString();
                        rowKd[50] = "";
                    }
                }

                if (rowKd[50].ToString().Length > 20)
                {
                    rowKd[50] = "";
                }

                if (rowKd[35].ToString() == "" | rowKd[35].ToString() == null)
                {
                    rowKd[35] = rowKd[4].ToString().ToUpper(); ;
                }

                if (Standort == 2 && Dictionaries.ZbdNrHBS2SDL.ContainsKey(rowKd[71].ToString()))
                {

                    rowKd[71] = Dictionaries.ZbdNrHBS2SDL[rowKd[71].ToString()];
                }

                else if (Dictionaries.ZbdNrVK.ContainsKey(rowKd[71].ToString()))
                {
                    rowKd[71] = Dictionaries.ZbdNrVK[rowKd[71].ToString()];
                }

                else
                {
                    rowKd[71] = "";
                }

                if (rowKd[93].ToString() != "0")
                {
                    rowKd[93] = "Ja";
                }

                else
                {
                    rowKd[93] = "Nein";
                }

                if (rowKd[52].ToString() == "0")
                {
                    rowKd[52] = "Nein";
                }

                else
                {
                    rowKd[52] = "Ja";
                }

                if (rowKd[73].ToString() == "N")
                {
                    rowKd[73] = "Ja";
                }

                else if (rowKd[73].ToString() == "J")
                {
                    rowKd[73] = "Ja";
                }
                else
                {
                    rowKd[73] = "Ja";
                }
                if (rowKd[78] != null && Dictionaries.VADict.ContainsKey(rowKd[78].ToString()))
                {
                    rowKd[78] = Dictionaries.VADict[rowKd[78].ToString()];
                }

                else
                {
                    rowKd[78] = "@";
                }

                if (rowKd[87] != null)
                {
                    string preisliste = string.Empty;
                    string rabattgruppe = string.Empty;

                    try
                    {
                        (rabattgruppe, preisliste) = operations.GetKundenrabattgruppe(rowKd[87].ToString());
                    }

                    catch (KeyNotFoundException knfe)
                    {
                        rowKd[87] = "@";
                    }

                    rowKd[87] = rabattgruppe;
                    rowKd[86] = preisliste;
                }

                rowKd[89] = "Ja";
                rowKd[91] = "@";

                if (operations.Betriebskalender.ContainsKey(rowKd[20].ToString()))
                {
                    rowKd[144] = operations.Betriebskalender[rowKd[20].ToString()];
                }

                if (rowKd[0].ToString().Length < 6)
                {
                    rowKd[0] = Standort + rowKd[0].ToString();
                }

                while (rowKd[37].ToString().Length < 3)
                {
                    rowKd[37] = "0" + rowKd[37].ToString();
                }

                if (rowKd[53].ToString() == "1")
                {
                    rowKd[53] = string.Empty;
                }
            }


            List<DataRow> dataRows = new List<DataRow>();
            DataTable outTable = dataTable.Clone();

            return dataTable;
        }


        public DataTable GetLieferant(string standort)
        {
            string startDate = string.Empty;
            startDate = DateTime.Now.AddDays(-4).ToString("yyyyMMdd");
            string getS_Lieferant = "";

            if (standort == "1")
            {
                string getS_Lieferant_SDL = "select " +
                    "kunden.kdn_kontonr," +
                    "'', " +
                    "anschrift.name_001, " +
                    "anschrift.land, " +
                    "anschrift.ort, " +
                    "anschrift.anssuch, " +
                    "anschrift.ans_suwo2, " +
                    "'@', " +
                    "'@'," +
                    "'@', " +                              //09
                    "'@', " +
                    "anschrift.name_002," +                //11
                    "anschrift.name_003, " +
                    "anschrift.plz," +
                    " '@' AS Expr3, " +
                    "anschrift.ans_teilort, " +
                    "'@' AS Expr4, " +
                    "anschrift.strasse, " +
                    "'@' AS Expr5, " +
                    "'Hausnummer' AS Hausnummer," +
                    "'@' AS Bundesland, " +
                    "anschrift.ans_pf_plz, " +
                    "anschrift.ans_postfach, " +
                    "'Ja' AS Expr8, " +
                    "anschrift.ans_email, " +
                    "anschrift.ans_homepage, " +
                    "anschrift.ans_telex, " +
                    "anschrift.ans_telefon, " +
                    "'' as 'Autotelefon'," +             //28
                    "anschrift.ans_telefax, " +
                    "'' as 'Telefon2'," +
                    "'' as 'Telefax2'," +
                    "'' as 'Längengrad'," +
                    "'' as 'Breitengrad'," +
                    "anssuch," +
                    "ans_suwo2," +
                    "'' as'Verteilergruppe'," +
                    "kdn_x_branche," +
                    "'@'," +
                    "'@'," +
                    "kdn_sprnr," +
                    "'@'," +
                    "kdn_kredkontonr," +
                    "ans_stnr," +
            /*40*/  "'0'," +
                    "ans_ustid," +
                    "'@'," +
                    "'9999'," +
            /*44*/  "kdn_zbnr," +
                    "kdn_kredlimit," +
                    "'1'," +
                    "'2'," +
                    "'3'," +
                    "'4'," +
                    "'5'," +
                    "'6'," +
            /*52*/  "kdn_lbdnr," +
                    "kdn_vsanr," +
                    "kdn_c_minbst," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "kdn_x_ust_wann," +
                    "''as 'Bankverbindung'," +
                    "'ja'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@', " +
                    "'@'," +
                    "'@' " +
                    "from anschrift, kunden " +
                    "where ansnr = kdn_lfdnr " +
                    "and ans_typ = 'K' " +
                    "and ans_aenderung > " + startDate + " " +
                    "GROUP BY kdn_kontonr";


                getS_Lieferant = getS_Lieferant_SDL;

            }

            if (standort == "2")
            {
                string getS_Lieferant_HBS = "select " +
                     "kunden.kdn_kontonr," +
                "'', " +
                "anschrift.name_001, " +
                "anschrift.land, " +
                "anschrift.ort, " +
                "anschrift.anssuch, " +
                "anschrift.ans_suwo2, " +
                "'@', " +
                "'@'," +
                "'@', " +                              //09
                "'@', " +
                "anschrift.name_002," +                //11
                "anschrift.name_003, " +
                "anschrift.plz," +
                " '@' AS Expr3, " +
                "anschrift.ans_teilort, " +
                "'@' AS Expr4, " +
                "anschrift.strasse, " +
                "'@' AS Expr5, " +
                "'Hausnummer' AS Hausnummer," +
                "'@' AS Bundesland, " +
                "anschrift.ans_pf_plz, " +
                "anschrift.ans_postfach, " +
                "'Ja' AS Expr8, " +
                "anschrift.ans_email, " +
                "anschrift.ans_homepage, " +
                "anschrift.ans_telex, " +
                "anschrift.ans_telefon, " +
                "'' as 'Autotelefon'," +             //28
                "anschrift.ans_telefax, " +
                "'' as 'Telefon2'," +
                "'' as 'Telefax2'," +
                "'' as 'Längengrad'," +
                "'' as 'Breitengrad'," +
                    "anssuch," +
                    "ans_suwo2," +
                    "'' as'Verteilergruppe'," +
                    "kdn_x_branche," +
                    "'@'," +
                    "'@'," +
                    "kdn_sprnr," +
                    "'@'," +
                    "kdn_kredkontonr," +
                    "ans_stnr," +
        /*40*/      "'0'," +
                    "ans_ustid," +
                    "'@'," +
                    "'9999'," +
        /*44*/      "kdn_zbnr," +
                    "kdn_kredlimit," +
                    "'1'," +                            //50
                    "'2'," +
                    "'3'," +
                    "'4'," +
                    "'5'," +
                    "'6'," +
        /*52*/      "kdn_lbdnr," +
                    "kdn_vsanr," +
                    "kdn_c_minbst," +
                    "'@'," +
                    "'@'," +
                    "'0'," +
                    "kdn_x_ust_wann," +
                    "''as 'Bankverbindung'," +
                    "'ja'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@', " +
                    "'@', " +
                    "'@' " +
                    "from anschrift,kunden " +
                    "where ansnr = kdn_lfdnr " +
                    "and ans_typ = 'K' " +
                //"and kdn_kontonr = 77325";
                "and ans_aenderung > " + startDate + " ";

                getS_Lieferant = getS_Lieferant_HBS;

            }


            else
            {
                getS_Lieferant = "select " +
                      "kunden.kdn_kontonr," +
                      "'', " +
                      "anschrift.name_001, " +
                      "anschrift.land, " +
                      "anschrift.ort, " +
                      "anschrift.anssuch, " +
                      "anschrift.ans_suwo2, " +
                      "'@', " +
                      "'@'," +
                      "'@', " +                              //09
                      "'@', " +
                      "anschrift.name_002," +                //11
                      "anschrift.name_003, " +
                      "anschrift.plz," +
                      " '@' AS Expr3, " +
                      "anschrift.ans_teilort, " +
                      "'@' AS Expr4, " +
                      "anschrift.strasse, " +
                      "'@' AS Expr5, " +
                      "'Hausnummer' AS Hausnummer," +
                      "'@' AS Bundesland, " +
                      "anschrift.ans_pf_plz, " +
                      "anschrift.ans_postfach, " +
                      "'Ja' AS Expr8, " +
                      "anschrift.ans_email, " +
                      "anschrift.ans_homepage, " +
                      "anschrift.ans_telex, " +
                      "anschrift.ans_telefon, " +
                      "'' as 'Autotelefon'," +             //28
                      "anschrift.ans_telefax, " +
                      "'' as 'Telefon2'," +
                      "'' as 'Telefax2'," +
                      "'' as 'Längengrad'," +
                      "'' as 'Breitengrad'," +
                      "anssuch," +
                      "ans_suwo2," +
                      "'' as'Verteilergruppe'," +
                      "kdn_x_branche," +
                      "'@'," +
                      "'@'," +
                      "kdn_sprnr," +
                      "'@'," +
                      "kdn_kredkontonr," +
                      "ans_stnr," +
                /*40*/"'0'," +
                      "ans_ustid," +
                      "'@'," +
                      "'9999'," +
                /*44*/"kdn_zbnr," +
                      "kdn_kredlimit," +
                      "'1'," +
                      "'2'," +
                      "'3'," +
                      "'4'," +
                      "'5'," +
                      "'6'," +
                /*52*/"kdn_lbdnr," +
                      "kdn_vsanr," +
                      "kdn_c_minbst," +
                      "'@'," +
                      "'@'," +
                      "'@'," +
                      "kdn_x_ust_wann," +
                      "''as 'Bankverbindung'," +
                      "'ja'," +
                      "'@'," +
                      "'@'," +
                      "'@'," +
                      "'@'," +
                      "'@'," +
                      "'@'," +
                      "'@'," +
                      "'@'," +
                      "'@'," +
                      "'@'," +
                      "'@'," +
                      "'@'," +
                      "'@'," +
                      "'@'," +
                      "'@'," +
                      "'@'," +
                      "'@'," +
                      "'@'," +
                      "'@'," +
                      "'@'," +
                      "'@'," +
                      "'@'," +
                      "'@'," +
                      "'@'," +
                      "'@'," +
                      "'@'," +
                      "'@'," +
                      "'@', " +
                      "'@'," +
                      "'@' " +
                      "from anschrift, kunden " +
                      "where ansnr = kdn_lfdnr " +
                      "and ans_typ = 'K' " +
                //"and ans_aenderung > " + DateTime.Now.AddDays(-4).ToString("yyyyMMdd") + " ";
                      "and kdn_kontonr = 80628 ";
            }

            OdbcConnection connection = null;

            if (standort == "1")
            {
                connection = OdbcSDL;
            }
            else
            {
                connection = OdbcHBS;
            }

            OdbcCommand command = new OdbcCommand(getS_Lieferant, connection);

            dataTable = new DataTable("__9_3__DU_Lieferant");
            dataTable = operations.WriteTable(command, dataTable, connection);

            foreach (DataRow row in dataTable.Rows)
            {
                if (standort == "2")
                {
                    if (Dictionaries.HBSLief2Change.ContainsKey(row[0].ToString()))
                    {
                        row[0] = Dictionaries.HBSLief2Change[row[0].ToString()];
                    }
                }
                if (Dictionaries.LieferantenAvisList.Contains(row[0].ToString()))
                {
                    row[47] = '0';
                }



                if (row[0].ToString().Length < 6)
                {
                    row[0] = row[0].ToString() + standort;
                }

                row[45] = operations.RemoveWhiteSpace(row[45].ToString());

                if (row[45].ToString() != string.Empty && row[45].ToString().Length > 3)
                {
                    if (!Dictionaries.LandUstidList.Contains(row[45].ToString().Substring(0, 2)))
                    {
                        MatchCollection matches = Regex.Matches(row[45].ToString(), @"\d+");
                        row[43] = matches[0].Value.ToString();
                        row[45] = "";
                    }
                }

                operations.NameTrimmer35(row[2].ToString(), row[11].ToString(), row[12].ToString(), row[16].ToString());

                if (row[3].ToString() == "DE")
                {
                    if (Dictionaries.BundeslandDict.ContainsKey(row[13].ToString()))
                    {
                        row[20] = Dictionaries.BundeslandDict[row[13].ToString()];
                    }
                    else
                    {
                        row[20] = "@";
                    }

                    row[17] = operations.CorrectStrasse(row[17].ToString());
                }

                else
                {
                    row[20] = "@";
                }

                if (row[3].ToString() == "CH")
                {
                    row[39] = row[41].ToString();
                    row[41] = "";
                }

                if (row[6].ToString() == null | row[6].ToString() == "")
                {
                    row[6] = row[4].ToString().ToUpper();
                }

                if (row[8] == null | row[8].ToString() == "")
                {
                    try
                    {
                        row[8] = Dictionaries.anredenDict[row[7].ToString()];
                    }
                    catch (Exception e)
                    {
                        WriteLog(e.Message);
                    }
                }

                try
                {
                    if (row[3].ToString() != "FR")
                    {
                        string street = row[17].ToString();
                        string hNr = row[19].ToString();
                        (street, hNr) = Operations.GetHnr(row[17].ToString());
                        row[17] = street;
                        row[19] = hNr;
                    }

                    else
                    {
                        row[19] = "@";
                    }

                    row[37] = Operations.SetBrancheNr(row[37].ToString());
                    row[38] = operations.Betriebskalender[row[20].ToString()];
                }
                catch (Exception e)
                {
                    WriteLog(e.Message);
                }

                if (Dictionaries.sprachenDict.ContainsKey(row[40].ToString()))
                {
                    row[40] = Dictionaries.sprachenDict[row[40].ToString()];
                }

                else
                {
                    row[40] = "@";
                }

                if (row[40].ToString() == "D" | row[3].ToString() == "DE" | row[3].ToString() == "CH" | row[3].ToString() == "AT")
                {
                    row[7] = "Sehr geehrte Damen und Herren";
                }

                else if (row[40].ToString() == "F" | row[4].ToString() == "FR")
                {
                    row[7] = "Mesdames, Messieurs";
                }

                else if (row[40].ToString() == "E" | row[4].ToString() == "GB")
                {
                    row[7] = "Dear Sir / Madam";
                }
                else if (row[40].ToString() == "DU" | row[4].ToString() == "NL")
                {
                    row[7] = "Geachte dames en heren";
                }

                else
                {
                    row[7] = row[7];
                }

                try
                {
                    row[45] = operations.RemoveWhiteSpace(row[45].ToString());
                    if (row[45].ToString() != string.Empty && row[45].ToString().Length > 3)
                    {
                        if (!Dictionaries.LandUstidList.Contains(row[45].ToString().Substring(0, 2)))
                        {
                            MatchCollection matches = Regex.Matches(row[45].ToString(), @"\d+");
                            row[43] = matches[0].Value.ToString();
                            row[45] = "";
                        }
                    }
                }
                catch (Exception e)
                {
                    row[45] = row[45].ToString();
                }

                if (Dictionaries.LbdDictEinkauf.ContainsKey(row[56].ToString()))
                {
                    row[56] = Dictionaries.LbdDictEinkauf[row[56].ToString()];
                }

                if (Dictionaries.VADictEinkauf.ContainsKey(row[57].ToString()))
                {
                    row[57] = Dictionaries.VADictEinkauf[row[57].ToString()];
                }
                else
                {
                    row[57] = "";
                }

                row[56] = operations.EinkaufLbdCheck(row[56].ToString(), row[57].ToString());
                row[46] = "4";


                if (row[62].ToString() != "0")
                {
                    row[62] = "Ja";
                }
                else
                {
                    row[62] = "Nein";
                }
                try
                {
                    if (standort == "1")
                    {
                        row[48] = Dictionaries.ZbdNrVK[row[48].ToString()];
                    }
                    if (standort == "2")
                    {
                        if (Dictionaries.ZbdNrHBS2SDL.ContainsKey(row[48].ToString()))
                        {
                            row[48] = Dictionaries.ZbdNrHBS2SDL[row[48].ToString()];
                        }

                        else
                        {
                            row[48] = Dictionaries.ZbdNrVK[row[48].ToString()];
                        }
                    }
                }
                catch (Exception e)
                {
                    row[48] = "__" + row[48].ToString();
                }
            }

            return dataTable;
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

        void BtnStartClick(object sender, RoutedEventArgs e)
        {
            if (System.Windows.MessageBox.Show("Soll der automatische Ablauf abgebrochen \n und neu gestartet werden?", "Restart", MessageBoxButton.YesNo) == MessageBoxResult.Yes)
            {
                try
                {
                    // Stoppe alle laufenden Tasks
                    cancellation.Cancel();

                    // Starte den Timer neu
                    StartTimer();
                }
                catch (Exception ex)
                {
                    // Fehlerbehandlung
                    System.Windows.MessageBox.Show($"Fehler beim Neustart: {ex.Message}", "Fehler", MessageBoxButton.OK, MessageBoxImage.Error);
                }
            }
        }

        void ResetMainWindow(MainWindow Window)
        {
            Window = new MainWindow();
            Window.Show();
        }
        void ButtonOptions_Click(object sender, RoutedEventArgs e)
        {
            Window1 window1 = new Window1();
            window1.Show();
        }
    }
}
