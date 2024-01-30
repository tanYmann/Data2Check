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
        private System.Timers.Timer timer = new System.Timers.Timer();
        static DataTable Atradius = new DataTable();
        static OdbcConnection OdbcSDL = new OdbcConnection("DSN=Parity_SDL;Pooling=true;");
        static OdbcConnection OdbcHBS = new OdbcConnection("DSN=Parity_HBS;Pooling=true;");
        static string preString { get; set; }
        static int Standort = 1;
        public OdbcConnection[] Connections = new OdbcConnection[] { OdbcSDL, OdbcHBS };
        static string ASCIIPath = @"\\bauer-gmbh.org\DFS\SL\PROALPHA\10. Team-DÜ, EDI, WF\Datenexport\transASCIIact\";
        static string DateFile = @"\\bauer-gmbh.org\DFS\SL\PROALPHA\10. Team-DÜ, EDI, WF\Datenexport\KundenLieferanten\LastDate.txt";
        static CancellationTokenSource cancellation = new CancellationTokenSource();
        const string RegistryKeyString = "Data2Check";
        NotifyIcon notifyIcon;
        static int testCounter = 0;
        TimeSpan target = new TimeSpan(2, 30, 0);
        static int TestCounter { get => testCounter; }
        static Operations operations = new Operations();
        static SQLMethods queriesMethods = new SQLMethods();
        bool WaitTxt = false;
        DateTime Endtime = new DateTime();
        static string LogFile = @"\\bauer-gmbh.org\DFS\SL\PROALPHA\10. Team-DÜ, EDI, WF\Datenexport\Logger.txt";
        static bool timerInitialized = false; 
        public async Task MainAsync()
        {
            while (!CheckNetwork())
            {
                SetText("Netzwerkverbindung nicht bereit.");
            }
                       
            await ExportData();
        }

        // MainWindoiw
        public MainWindow()
        {
            InitializeComponent();
            InitializeNotifyIcon();
            _ = Task.Run(() => ExportData());
        }

        void WriteLogFile(string line)
        {
            using (FileStream stream = new FileStream(LogFile, FileMode.Append, FileAccess.Write))
            {
                using (StreamWriter writer = new StreamWriter(stream))
                {
                    writer.WriteLine(DateTime.Now.Date.ToString() + " : " + " [ " + DateTime.Now.Hour.ToString() + ":" + DateTime.Now.Minute.ToString() + " : " + line);
                }
            }
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
                                                
        // Endzeit des Timers
        DateTime SetEndTime(TimeSpan span)
        {
            DateTime now = DateTime.Now;
            now += span;
            return now;
        }

        // Vergangene Zeit
        async void TimerElapsed(object sender, ElapsedEventArgs e)
        {
            TimeSpan remainingTime = NextExecution(Endtime);
            System.Windows.Application.Current.Dispatcher.Invoke(() => SetRemainingTime(remainingTime));

            if (remainingTime <= TimeSpan.Zero)
            {
                timer.Stop();
                await Task.Run(() => ExportData());
            }
        }

        // Anzeige der verbleibenden Zeit bis zur nächsten Ausführung
        void SetRemainingTime(TimeSpan remainingTime)
        {
            txtRemainingTime.Text = $"Zeit bis zum nächsten Export: {remainingTime.Hours}h {remainingTime.Minutes}m {remainingTime.Seconds}s";
        }

        // Zeitspanne bis zur nächsten Ausführung
        TimeSpan NextExecution(DateTime targetTime)
        {
            DateTime now = DateTime.Now;
            DateTime targetDateTime = new DateTime(now.Year, now.Month, now.Day, targetTime.Hour , targetTime.Minute, targetTime.Second );

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
            
            
            TimeSpan span = now - targetTime;
            double doubl = span.TotalMilliseconds;

            if (span.TotalMilliseconds < -1)
            {
                doubl = span.TotalMilliseconds * (-1);
                
            }

            span = TimeSpan.FromMilliseconds(doubl);

            return span;
        }

        // Autostart überprüfen
        void SetRegistryKey()
        {
            RegistryKey key = Registry.CurrentUser.OpenSubKey("SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Run", true);

            if (key.GetValue("Data2Check") == null)
            {
                key.SetValue(key.Name, System.Reflection.Assembly.GetExecutingAssembly().Location);
            }

            SetText("Key in Registrierung geprüft.\n");
        }

        //  Get und Set LastDate
        (string Date2Use, string DateToWrite) GetLastDate()
        {
            string dateRead = string.Empty;
            string dateWrite = string.Empty;

            using (FileStream fstream = new FileStream(DateFile, FileMode.Open, FileAccess.ReadWrite))
            {
                using (StreamReader sreader = new StreamReader(fstream))
                {
                    sreader.BaseStream.Position = 0;
                    dateRead = sreader.ReadLine();
                }
            }
            string dateDay;
            string dateMonth;
            int day = DateTime.Now.Day;
            int month = DateTime.Now.Month;
            int year = DateTime.Now.Year;

            if (day < 10)
            {
                dateDay = "0" + day.ToString();
            }
            else
            {
                dateDay = day.ToString();
            }

            if (month < 10)
            {
                dateMonth = "0" + month.ToString();
            }
            else
            {
                dateMonth = month.ToString();
            }

            dateWrite = year.ToString() + dateMonth + dateDay;

            using (FileStream fstream = new FileStream(DateFile, FileMode.Open, FileAccess.ReadWrite))
            using (StreamWriter writer = new StreamWriter(fstream))
            {
                writer.BaseStream.Position = 0;
                writer.WriteLine(dateWrite);
            }

            return (dateRead, dateWrite);
        }


        // Prüfen der Netzwerkverbindung
        bool CheckNetwork()
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
        async Task <bool> checkOdbc(OdbcConnection connection)
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
        async Task ExportData()
        {
            Tables tables = new Tables();
            Standort = 1;
            CancellationToken token = cancellation.Token;
            DataTable dataTable = new DataTable();
            operations.FillAtradius(Atradius);
            
            foreach (OdbcConnection conn in Connections)
            {
                await checkOdbc(conn);

                if (token.IsCancellationRequested)
                {
                    return;
                }

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
                        if (token.IsCancellationRequested)
                        {
                            return;
                        }

                        dataTable = new DataTable(key.Key.ToString());

                        using (OdbcCommand command = new OdbcCommand(key.Value, conn))
                        {
                            if (conn.State != ConnectionState.Open)
                            {
                                conn.Open();
                            }

                            if (key.Key.ToString() == "DU_Kunde")
                            {
                                string query = string.Format(key.Value, GetLastDate().Date2Use);
                                Operations operations = new Operations();
                                DataTable kobensen = new DataTable();
                                operations.FillUstidKobensen(kobensen);
                                command.CommandText = query;
                                dataTable = operations.WriteTable(command, dataTable);

                                foreach (DataRow rowKd in dataTable.Rows)
                                {
                                    try
                                    {
                                        if (Atradius.Rows.Contains(rowKd[0].ToString()))
                                        {
                                            foreach (DataRow row in Atradius.Rows)

                                            {
                                                if (row[0].ToString() == rowKd[0].ToString())
                                                {
                                                    rowKd[79] = "216426";
                                                    rowKd[80] = row[1];
                                                    rowKd[81] = row[4];
                                                    rowKd[82] = "B";
                                                    rowKd[83] = row[5];
                                                }
                                            }
                                        }
                                        else
                                        {
                                            rowKd[79] = "";
                                            rowKd[80] = "";
                                            rowKd[81] = "";
                                            rowKd[82] = "";
                                            rowKd[83] = "";
                                        }

                                        if (kobensen.Rows.Contains(rowKd[0].ToString()))
                                        {
                                            foreach (DataRow row in kobensen.Rows)
                                            {
                                                if (row[0].ToString() == rowKd[0].ToString())
                                                {
                                                    rowKd[47] = operations.RemoveWhiteSpace(row[2].ToString());
                                                }
                                            }
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        rowKd[43] = "";
                                        WriteLogFile("Source     : " + ex.Source + "\n" +
                                                     "TargetSite : " + ex.TargetSite + "\n" +
                                                     "StackTrace : " + ex.StackTrace + "\n" +
                                                     "Message    : " + ex.Message + "\n");
                                    }

                                    string hNr = string.Empty;
                                    string street = rowKd[18].ToString();

                                    if (Dictionaries.JungheinrichDict.ContainsKey(rowKd[0].ToString()))
                                    {
                                        rowKd[0] = Dictionaries.JungheinrichDict[rowKd[0].ToString()];
                                    }
                                    else
                                    {
                                        rowKd[0] = Standort.ToString() + rowKd[0].ToString();
                                    }

                                    rowKd[7] = rowKd[5].ToString().ToUpper();
                                    rowKd[34] = Operations.SetBrancheNr(rowKd[34].ToString());
                                    rowKd[18] = operations.CorrectStrasse(rowKd[18].ToString());

                                    if (rowKd[18] != null)
                                    {
                                        if (rowKd[4].ToString() != "FR")
                                        {
                                            try
                                            {
                                                (street, hNr) = Operations.GetHnr(rowKd[18].ToString());
                                                rowKd[18] = street;
                                                rowKd[20] = hNr;
                                            }
                                            catch (Exception ex)
                                            {
                                                WriteLogFile("Source     : " + ex.Source + "\n" +
                                                    "TargetSite : " + ex.TargetSite + "\n" +
                                                    "StackTrace : " + ex.StackTrace + "\n" +
                                                    "Message    : " + ex.Message + "\n");

                                            }
                                        }
                                        else
                                        {

                                            rowKd[20] = "";
                                        }
                                    }
                                    int out43 = 0;

                                    if (int.TryParse(rowKd[43].ToString(), out out43))
                                    {
                                        rowKd[43] = operations.DateConvert(out43.ToString());
                                    }
                                    else
                                    {
                                        rowKd[43] = "";
                                    }

                                    rowKd[39] = "Ja";

                                    if (rowKd[50] != null && rowKd[50].ToString() != "@")
                                    {
                                        if (rowKd[50].ToString() != "0")
                                        {
                                            rowKd[50] = Standort.ToString() + rowKd[50].ToString();
                                        }

                                        else
                                        {
                                            rowKd[50] = "";
                                        }
                                    }

                                    if (rowKd[53].ToString() != "0")
                                    {

                                        if (Dictionaries.VerbandDict.ContainsKey(rowKd[53].ToString()))
                                        {
                                            rowKd[53] = Dictionaries.VerbandDict[rowKd[53].ToString()];
                                        }
                                        else
                                        {
                                            rowKd[53] = "";
                                        }
                                    }
                                    else
                                    {
                                        rowKd[53] = string.Empty;
                                    }


                                    if (rowKd[53].ToString() == "195160" | rowKd[53].ToString() == "381450" | rowKd[53].ToString() == "194330" | rowKd[53].ToString() == "196930")
                                    {
                                        rowKd[57] = operations.GetNrVerb(rowKd[12].ToString(), rowKd[13].ToString(), rowKd[53].ToString());
                                    }

                                    if (rowKd[4].ToString().ToUpper() == "DE")
                                    {
                                        try
                                        {
                                            rowKd[21] = Dictionaries.BundeslandDict[rowKd[14].ToString()];
                                            rowKd[18] = operations.CorrectStrasse(rowKd[18].ToString());
                                        }

                                        catch (KeyNotFoundException ex)
                                        {
                                            rowKd[21] = "";
                                            WriteLogFile("Source     : " + ex.Source + "\n" +
                                                    "TargetSite : " + ex.TargetSite + "\n" +
                                                    "StackTrace : " + ex.StackTrace + "\n" +
                                                    "Message    : " + ex.Message + "\n");
                                        }
                                    }

                                    else
                                    {
                                        rowKd[21] = "@";
                                    }

                                    rowKd[35] = operations.GetRegion(rowKd[4].ToString(), rowKd[14].ToString());

                                    if (Dictionaries.sprachenDict.ContainsKey(rowKd[37].ToString()))
                                    {
                                        rowKd[37] = Dictionaries.sprachenDict[rowKd[37].ToString()];
                                    }

                                    else
                                    {
                                        rowKd[37] = "@";
                                    }

                                    if (rowKd[37].ToString() == "D" | rowKd[4].ToString() == "DE" | rowKd[4].ToString() == "CH" | rowKd[4].ToString() == "AT")
                                    {
                                        rowKd[8] = "Sehr geehrte Damen und Herren";
                                    }
                                    else if (rowKd[37].ToString() == "F" | rowKd[4].ToString() == "FR")
                                    {
                                        rowKd[8] = "Mesdames, Messieurs";
                                    }
                                    else if (rowKd[37].ToString() == "E")
                                    {
                                        rowKd[8] = "Dear Sir / Madam";
                                    }
                                    else if (rowKd[37].ToString() == "DU")
                                    {
                                        rowKd[8] = "Geachte dames en heren";
                                    }

                                    else
                                    {
                                        rowKd[8] = rowKd[8];
                                    }

                                    rowKd[47] = operations.RemoveWhiteSpace(rowKd[47].ToString());

                                    if (rowKd[47].ToString() != string.Empty && rowKd[47].ToString().Length > 3)
                                    {
                                        if (!Dictionaries.LandUstidList.Contains(rowKd[47].ToString().Substring(0, 2)))
                                        {
                                            rowKd[45] = rowKd[47].ToString();
                                            rowKd[47] = string.Empty;
                                        }
                                    }

                                    if (rowKd[32].ToString() == "" | rowKd[32].ToString() == null)
                                    {
                                        rowKd[32] = rowKd[5].ToString().ToUpper(); ;
                                    }

                                    if (Standort == 2 && Dictionaries.ZbdNrHBS2SDL.ContainsKey(rowKd[68].ToString()))
                                    {

                                        rowKd[68] = Dictionaries.ZbdNrHBS2SDL[rowKd[68].ToString()];
                                    }
                                    else if (Dictionaries.ZbdNrVK.ContainsKey(rowKd[68].ToString()))
                                    {
                                        rowKd[68] = Dictionaries.ZbdNrVK[rowKd[68].ToString()];
                                    }
                                    else
                                    {
                                        rowKd[68] = "@";
                                    }

                                    if (rowKd[93].ToString() != "0")
                                    {
                                        rowKd[93] = "Ja";
                                    }

                                    else
                                    {
                                        rowKd[93] = "Nein";
                                    }

                                    if (rowKd[49].ToString() == "0")
                                        rowKd[49] = "Nein";


                                    else
                                    {
                                        rowKd[49] = "Ja";
                                    }

                                    if (rowKd[70].ToString() == "N")
                                    {
                                        rowKd[70] = "Ja";
                                    }

                                    else if (rowKd[70].ToString() == "J")
                                    {
                                        rowKd[70] = "Nein";
                                    }

                                    if (rowKd[76] != null && Dictionaries.VADict.ContainsKey(rowKd[76].ToString()))
                                    {
                                        rowKd[76] = Dictionaries.VADict[rowKd[76].ToString()];
                                    }

                                    else
                                    {
                                        rowKd[76] = "@";
                                    }

                                    if (rowKd[84] != null)
                                    {
                                        try
                                        {
                                            rowKd[84] = operations.GetPreisliste(rowKd[84].ToString());
                                        }

                                        catch (KeyNotFoundException ex)
                                        {
                                            WriteLogFile("Source     : " + ex.Source + "\n" +
                                                    "TargetSite : " + ex.TargetSite + "\n" +
                                                    "StackTrace : " + ex.StackTrace + "\n" +
                                                    "Message    : " + ex.Message + "\n");
                                            rowKd[84] = "";
                                        }
                                    }

                                    if (rowKd[85] != null)
                                    {
                                        try
                                        {
                                            rowKd[85] = operations.GetKundenrabattgruppe(rowKd[85].ToString());
                                        }

                                        catch (KeyNotFoundException ex)
                                        {
                                            rowKd[85] = "";
                                            WriteLogFile("Source     : " + ex.Source + "\n" +
                                                    "TargetSite : " + ex.TargetSite + "\n" +
                                                    "StackTrace : " + ex.StackTrace + "\n" +
                                                    "Message    : " + ex.Message + "\n");
                                        }
                                    }

                                    rowKd[86] = "Ja";
                                }
                            }
                            else if (key.Key.ToString() == "DU_Lieferant")
                            {
                                string query = string.Format(key.Value, GetLastDate().Date2Use);
                                Operations operations = new Operations();
                                DataTable kobensen = new DataTable();
                                dataTable = new DataTable("DU_Lieferant");
                                command.CommandText = query;
                                dataTable = operations.WriteTable(command, dataTable);

                                foreach (DataRow row in dataTable.Rows)
                                {
                                    row[0] = row[0].ToString() + Standort;
                                    row[41] = operations.RemoveWhiteSpace(row[41].ToString());

                                    if (row[41].ToString() != string.Empty && row[41].ToString().Length > 3)
                                    {
                                        if (!Dictionaries.LandUstidList.Contains(row[41].ToString().Substring(0, 2)))
                                        {
                                            row[39] = row[41].ToString();
                                            row[41] = "";

                                        }

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
                                                WriteLogFile(e.Message);
                                            }
                                        }

                                        try
                                        {
                                            if (row[3].ToString() != "FR")
                                            {
                                                string street = "";
                                                string HNr = "";

                                                (street, HNr) = Operations.GetHnr(row[17].ToString());
                                                row[17] = street;
                                                row[19] = HNr; ;
                                            }

                                            else
                                            {
                                                row[19] = "";
                                            }

                                            row[33] = Operations.SetBrancheNr(row[33].ToString());
                                            row[34] = Dictionaries.Betriebskalender[row[20].ToString()];
                                        }
                                        catch (Exception ex)
                                        {
                                            SetText($"Error: {ex.Message}");
                                            WriteLogFile("Source     : " + ex.Source + "\n" +
                                                         "TargetSite : " + ex.TargetSite + "\n" +
                                                         "StackTrace : " + ex.StackTrace + "\n" +
                                                         "Message    : " + ex.Message + "\n");

                                        }

                                        if (Dictionaries.sprachenDict.ContainsKey(row[36].ToString()))
                                        {
                                            row[36] = Dictionaries.sprachenDict[row[36].ToString()];
                                        }

                                        else
                                        {
                                            row[36] = "@";
                                        }

                                        if (row[36].ToString() == "D" | row[4].ToString() == "DE" | row[4].ToString() == "CH" | row[4].ToString() == "AT")
                                        {
                                            row[7] = "Sehr geehrte Damen und Herren";
                                        }

                                        else if (row[36].ToString() == "F" | row[4].ToString() == "FR")
                                        {
                                            row[7] = "Mesdames, Messieurs";
                                        }

                                        else if (row[36].ToString() == "E" | row[4].ToString() == "GB")
                                        {
                                            row[7] = "Dear Sir / Madam";
                                        }
                                        else if (row[36].ToString() == "DU" | row[4].ToString() == "NL")
                                        {
                                            row[7] = "Geachte dames en heren";
                                        }

                                        else
                                        {
                                            row[7] = row[7];
                                        }

                                        try
                                        {
                                            row[41] = operations.RemoveWhiteSpace(row[41].ToString());
                                        }

                                        catch (Exception ex)
                                        {
                                            row[41] = row[41].ToString();
                                            SetText($"Error: {ex.Message}");
                                            WriteLogFile("Source     : " + ex.Source + "\n" +
                                                         "TargetSite : " + ex.TargetSite + "\n" +
                                                         "StackTrace : " + ex.StackTrace + "\n" +
                                                         "Message    : " + ex.Message + "\n");

                                        }

                                        if (Dictionaries.LbdDictEinkauf.ContainsKey(row[52].ToString()))
                                        {
                                            row[52] = Dictionaries.LbdDictEinkauf[row[52].ToString()];
                                        }

                                        if (Dictionaries.VADictEinkauf.ContainsKey(row[53].ToString()))
                                        {
                                            row[53] = Dictionaries.VADictEinkauf[row[53].ToString()];
                                        }
                                        else
                                        {
                                            row[53] = "@";
                                        }

                                        row[52] = operations.EinkaufLbdCheck(row[52].ToString(), row[53].ToString());
                                        row[42] = "4";
                                        row[43] = "2";

                                        if (row[58].ToString() != "0")
                                        {
                                            row[58] = "Ja";
                                        }
                                        else
                                        {
                                            row[58] = "Nein";
                                        }

                                        try
                                        {
                                            if (Standort == 1)
                                            {
                                                row[44] = Dictionaries.ZbdNrVK[row[44].ToString()];
                                            }
                                            if (Standort == 2)
                                            {
                                                if (Dictionaries.ZbdNrHBS2SDL.ContainsKey(row[44].ToString()))
                                                {
                                                    row[44] = Dictionaries.ZbdNrHBS2SDL[row[44].ToString()];
                                                }
                                                else
                                                {
                                                    row[44] = Dictionaries.ZbdNrVK[row[44].ToString()];
                                                }
                                            }
                                        }
                                        catch (Exception ex)
                                        {
                                            row[44] = "__" + row[44].ToString();
                                            SetText($"Error: {ex.Message}");
                                            WriteLogFile("Source     : " + ex.Source + "\n" +
                                                         "TargetSite : " + ex.TargetSite + "\n" +
                                                         "StackTrace : " + ex.StackTrace + "\n" +
                                                         "Message    : " + ex.Message + "\n");
                                        }
                                    }

                                }
                            }
                            else
                            {
                                dataTable.Load(command.ExecuteReader());
                            }
                            
                            queriesMethods.Table2CSV(dataTable, ASCIIPath, preString);
                            await Task.Delay(100);
                            SetText("[" + DateTime.Now.ToString() + "]" + preString + dataTable.TableName + " wurde exportiert. \n");
                            UpdateProgressBar(10);
                        }
                    }
                }
                catch (Exception ex)
                {
                    SetText($"Error: {ex.Message}");
                    WriteLogFile("Source     : " + ex.Source + "\n" +
                                 "TargetSite : " + ex.TargetSite + "\n" +
                                 "StackTrace : " + ex.StackTrace + "\n" +
                                 "Message    : " + ex.Message + "\n");
                }
                finally
                {
                    if (conn.State == ConnectionState.Open)
                    {
                        conn.Close();
                    }

                    Standort++;
                }
            

                int i = 0;
                UpdateProgressBar(10);
                UpdateProgressBar(10);
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

            DateTime time = DateTime.Now;
            //DateTime time = new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.Now.Day,2,30,0);
            //time = time.AddDays(1);
            time = time.AddMinutes(3);
            TimeSpan timeSpan = DateTime.Now.Subtract(time);
            double spanDouble = timeSpan.TotalMilliseconds;
            
            if (timeSpan.TotalMilliseconds < 0)
            {
                 spanDouble = timeSpan.TotalMilliseconds * (-1);                
            }

            TimeSpan time1 = TimeSpan.FromMilliseconds(spanDouble);  
            Endtime = SetEndTime(time1);
            InitializeTimer();
        }

        // Notify-Icon beim Minimieren
        void InitializeNotifyIcon()
        {
            notifyIcon = new NotifyIcon
            {
                Icon = SystemIcons.Application,
                Visible = true
            };

            notifyIcon.Click += NotifyIcon_Click;
        }

        // Click auf Icon
        void NotifyIcon_Click(object sender, EventArgs e)
        {
            Show();
            WindowState = WindowState.Normal;
        }

        // Minimieren des Fensters
        void OnStateChanged(EventArgs e)
            {
                base.OnStateChanged(e);

                if (WindowState == WindowState.Minimized)
                {
                    Hide();
                    notifyIcon.ShowBalloonTip(1000, "Anwendung minimiert", "Die Anwendung wurde minimiert.", ToolTipIcon.Info);
                }
            }

        // Schließen des Fensters
        void OnClosing(System.ComponentModel.CancelEventArgs e)
            {
                base.OnClosing(e);
                // Icon bei Schließen des Fensters entfernen
                notifyIcon.Dispose();
            }

        // Methode zum Anzeigen des Status Text
        void SetText(string text)
        {
            System.Windows.Application.Current.Dispatcher.Invoke(() =>
            {
                txtStatus.Text += text;
                txtStatus.BringIntoView();
            });
        }

        // Standortkürzel für Dateinamen
        void SetPreString(string connectionString)
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
        void UpdateProgressBar(int value)
            {
                System.Windows.Application.Current.Dispatcher.Invoke(() =>
                {
                    progressbar.Value += value;
                    progressbar.BringIntoView();
                });
            }

        // Shortcut im Autostart anlegen
        void CreateShortcutInAutostart()
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
        void ButtonCreateShortcut_Click(object sender, RoutedEventArgs e)
        {
            CreateShortcutInAutostart();
        }

        // Button zum manuellen Starten des Datenexports
        void BtnStartClick(object sender, RoutedEventArgs e)
        {
            Task.Run(() => ExportData());

        }

        // Button zum Öffnen der Optionen
        void ButtonOptions_Click(object sender, RoutedEventArgs e)
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


