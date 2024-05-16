using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;
using System.Drawing;
using System.IO;
using System.Net.NetworkInformation;
using System.ServiceProcess;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Forms;
using System.Windows.Media;
using System.Windows.Threading;
using Microsoft.Win32;

namespace Data2Checker
{
    public partial class MainWindow : Window
    {

        //Variablen
        SQLMethods methods = new SQLMethods();
        static DataTable Atradius = new DataTable();
        static OdbcConnection OdbcSDL = new OdbcConnection("DSN=Parity_SDL;Pooling=true;");
        static OdbcConnection OdbcHBS = new OdbcConnection("DSN=Parity_HBS;Pooling=true;");
        static string preString { get; set; }
        static int Standort = 1;
        public OdbcConnection[] Connections = new OdbcConnection[] { OdbcSDL, OdbcHBS };
        static string ASCIIPath = @"\\bauer-gmbh.org\DFS\SL\PROALPHA\10. Team-DÜ, EDI, WF\Datenexport\transASCIIact\test\";
        static string DateFile = @"\\bauer-gmbh.org\DFS\SL\PROALPHA\10. Team-DÜ, EDI, WF\Datenexport\KundenLieferanten\LastDate.txt";
        static CancellationTokenSource cancellation = new CancellationTokenSource();
        const string RegistryKeyString = "Data2Checker";
        NotifyIcon notifyIcon;
        static int testCounter = 0;
        TimeSpan target = new TimeSpan(2, 30, 0);
        static int TestCounter { get => testCounter; }
        static Operations operations = new Operations();
        static SQLMethods queriesMethods = new SQLMethods();
        bool WaitTxt = false;
        private static readonly DateTime dateTime = new DateTime();
        public DateTime Endtime = dateTime;
        static string LogFile = @"\\bauer-gmbh.org\DFS\SL\PROALPHA\10. Team-DÜ, EDI, WF\Datenexport\Logger.txt";
        //static bool timerInitialized = false;
        Window1 Window1 = new Window1();
        private DispatcherTimer timer;
        private TimeSpan TimeSpan;

        //asynchrone Mainmethode
        public async Task MainAsync()
        {
            while (!CheckNetwork())
            {
                SetText("Netzwerkverbindung nicht bereit.");
            }

            await ExportData();
        }
      
        public MainWindow()
        {
            InitializeComponent();

            // Start the timer
            CountdownTimer(TimeSpan);
            InitializeNotifyIcon();
            SetRegistryKey();
            _ = Task.Run(async () => await ExportData());
        }

        private void StartTimer()
        {
            // Create a new timer
            timer = new DispatcherTimer();
            timer.Interval = TimeSpan.FromSeconds(1); // Set the interval to 24 hours
            timer.Tick += Timer_Tick;

            // Calculate the time until 7:00 AM
            DateTime now = DateTime.Now;
            DateTime nextExecutionTime = new DateTime(now.Year, now.Month, now.Day + 1, 7, 0, 0);
            if (nextExecutionTime < now)
            {
                nextExecutionTime = nextExecutionTime.AddDays(1);
            }
            TimeSpan timeUntilNextExecution = nextExecutionTime - now;

            // Start the countdown timer
            CountdownTimer(timeUntilNextExecution);

            // Start the main timer
            timer.Start();
        }

        private void Timer_Tick(object sender, EventArgs e)
        {
            // Stop the main timer
            timer.Stop();

            // Execute the task
            Task.Run(async () => await ExportData());

            // Start the timer again
            StartTimer();
        }

        private void CountdownTimer(TimeSpan timeUntilNextExecution)
        {
            // Create a new timer for the countdown
            DispatcherTimer countdownTimer = new DispatcherTimer();
            countdownTimer.Interval = TimeSpan.FromSeconds(1);
            countdownTimer.Tick += (sender, e) =>
            {
                // Update the countdown label
                TimeSpan remainingTime = timeUntilNextExecution - DateTime.Now.TimeOfDay;
                txtRemainingTime.Text = $"Zeit bis zum nächsten Export: {remainingTime.Hours}h {remainingTime.Minutes}m {remainingTime.Seconds}s";

                // Stop the countdown timer when the time reaches 0
                if (remainingTime <= TimeSpan.Zero)
                {
                    countdownTimer.Stop();
                }
            };

            // Start the countdown timer
            countdownTimer.Start();
        }

     

        //Schreiben in den Logfile
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

        // Endzeit des Timers
        DateTime SetEndTime(TimeSpan span)
        {
            DateTime now = DateTime.Now;
            now += span;
            return now;
        }

        // Autostart überprüfen
        void SetRegistryKey()
        {
            RegistryKey key = Registry.CurrentUser.OpenSubKey("SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Run", true);

            if (key.GetValue("Data2Checker") == null)
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
            CancellationToken token = cancellation.Token;

            operations.FillAtradius(Atradius);

            foreach (OdbcConnection conn in Connections)
            {
                DataTable dataTable = new DataTable();
                Standort = int.Parse(methods.GetStandort(conn));
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
                            {/*
                                string konto = string.Empty;
                                string query = string.Format(key.Value, GetLastDate().Date2Use,GetLastDate().DateToWrite);
                                Operations operations = new Operations();
                                DataTable kobensen = new DataTable();
                                operations.FillUstidKobensen(kobensen);
                                command.CommandText = query;
                                dataTable = operations.WriteTable(command, dataTable);

                                foreach (DataRow rowKd in dataTable.Rows)
                                {
                                    konto = rowKd[0].ToString();

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
                                */
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
                                try
                                {
                                    dataTable.Load(command.ExecuteReader());
                                }
                                catch (Exception ex)
                                {
                                    txtStatus.Text = ex.Message + ex.Source;
                                    txtStatus.BringIntoView();
                                }
                            }

                            queriesMethods.Table2CSV(dataTable, ASCIIPath, preString);

                            await Task.Delay(100);
                            SetText("[" + DateTime.Now.ToString() + "] : " + preString + dataTable.TableName + "wurde exportiert. \n");

                            while (conn.State != ConnectionState.Closed)
                            {
                                try
                                {
                                    conn.Close();
                                }
                                catch (Exception ex)
                                {

                                }
                            }

                        }
                        UpdateProgressBar(10);
                        UpdateProgressBar(10);
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
            }
        }

        //Initialisierung des Timers für die Zeit bis zur nächsten Ausführung
        private void InitializeTimer()
        {
            TimeSpan span = new TimeSpan(1000);
            timer = new DispatcherTimer()
            {
                Interval = span // Timer-Intervall in Millisekunden (hier 1 Sekunde)
                

            };

            Endtime = DateTime.Now.AddMinutes(60);
            
            timer.Start();
        }

        // Vergangene Zeit
        async void TimerElapsed(object sender, ElapsedEventArgs e)
        {
            
            TimeSpan remainingTime = NextExecution(Endtime);
            System.Windows.Application.Current.Dispatcher.Invoke(() => 
            SetRemainingTime(remainingTime));

            if (remainingTime <= TimeSpan.Zero)
            {
                timer.Stop();
                await System.Windows.Application.Current.Dispatcher.InvokeAsync(() =>
                {
                    Task.Run(async ()=> await ExportData());
                });

            }
        }

        // Anzeige der verbleibenden Zeit bis zur nächsten Ausführung
        void SetRemainingTime(TimeSpan remainingTime)
        {
          
        }

        // Zeitspanne bis zur nächsten Ausführung
        public TimeSpan NextExecution(DateTime targetTime)
        {
            DateTime now = DateTime.Now;
            DateTime targetDateTime = new DateTime(targetTime.Year, targetTime.Month, targetTime.Day, targetTime.Hour,targetTime.Minute,targetTime.Second);

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

            TimeSpan span = targetTime-now;


            return span;
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
                FileInfo shortcutFileInfo = new FileInfo(startupFolderPath + "\\Data2Checker.lnk");

                if (shortcutFileInfo.Exists)
                {
                    shortcutFileInfo.Delete();
                }

                using (StreamWriter writer = new StreamWriter(shortcutFileInfo.FullName))
                {
                    writer.WriteLine("[Data2Checker]");
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
            _ = Task.Run(async() => await ExportData());
        }

        // Button zum Öffnen der Optionen
        void ButtonOptions_Click(object sender, RoutedEventArgs e)
        {
            if(Window1.WindowState != WindowState.Maximized)
            {
               
            }
            Window1.Activate();
            Window1.Show();
            if (!Window1.IsVisible)
            {
                Window1.BringIntoView();
            }
            else
            {
                Window1.Show();
            }

        }
        /*
        public DataTable GetKunden() 
        { 
            string limit = "";

            DataTable dataTable = new DataTable("DU_Kunde");

            if (System.Windows.MessageBox.Show("Für 9.3 exportieren ?", "Versionswahl", MessageBoxButton.YesNo) == MessageBoxResult.No)
            {
                string getKunde = "SELECT " +
                    "kunden.kdn_kontonr," +
                    "'@' as Profitcenter, " +
                    "'', " +
                    "anschrift.name_001, " +
                    "anschrift.land, " +
                    "anschrift.ort, " +
                    "anschrift.anssuch, " +
                    "anschrift.ans_suwo2, " +
                    "'@', " +
                    "'@'," +
                    "'@', " +
                    "'@', " +
                    "anschrift.name_002," +
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
                    "anschrift.ans_telefax, " +
                    "'@' AS Expr9, " +
                    "anschrift.anssuch AS Expr10, " +
                    "anschrift.ans_suwo2 AS Expr11, " +
                    "'@' AS Expr12," +
                    "kunden.kdn_x_branche, " +
                    "'@' AS Expr13, " +
                    "'@', " +
                    "kdn_sprnr, " +
                    "'@' AS Expr14, " +
                    "'@' AS Expr15, " +
                    "'@' AS Expr16, " +
                    "'@' AS Expr17, " +
                    "'@' AS Expr18, " +
                    "kunden.kdn_erstellt, " +
                    "persktn.pkt_kdn_lief_nr, " +
                    "anschrift.ans_stnr, " +
                    "'0'," +
                    "anschrift.ans_ustid, " +
                    "'@' AS Expr19, " +
                    "kunden.kdn_faktkz, " +
                    "persktn.pkt_rgzahler, " +
                    "'@' AS Expr21, " +
                    "'@' AS Expr22, " +
                    "kunden.kdn_ekvnr, " +
                    "(select f1e_x_ekname from f1ekverband where f1ekverband.f1e_x_eknr = kunden.kdn_ekvnr limit 1) AS 'f1ekname', " +
                    "'@' AS Expr24, " +
                    "'@' AS Expr25, " +
                    "'@' AS Expr26, " +
                    "'@' AS Expr27, " +
                    "'@' AS Expr28, " +
                    "'1', " +
                    "'2', " +
                    "'3', " +
                    "'4', " +
                    "'5', " +
                    "'6', " +
                    "'3', " +
                    "'@' AS Expr36, " +
                    "kdn_zbnr, " +
                    "kunden.kdn_kredlimit, " +
                    "kdn_c_ohneklm, " +
                    "'2', " +
                    "'@' AS Expr39, " +
                    "'@' AS Expr40, " +
                    "'@' AS Expr41, " +
                    "'@' AS Expr42, " +
                    "kunden.kdn_vsanr, " +
                    "'1', " +
                    "'1', " +
                    "'@', " +
                    "'@' AS Expr46, " +
                    "kunden.kdn_kredvers, " +
                    "'@' AS Expr48, " +
                    "'@' AS Expr49, " +
                    "kunden.kdn_x_adrsel, " +
                    "kdn_x_adrsel||'_'||anschrift.land||'_'||kdn_vertrnr_001, " +
                    "kdn_x_rabattausw, " +
                    "'Ja' AS Expr50, " +
                    "'Ja' AS Expr51, " +
                    "'@' AS Expr52, " +
                      "'', " +
                      "'', " +
                      "'0', " +
                      "kdn_x_ust_wann " +
                    "FROM kunden, anschrift,persktn " +
                    "WHERE (persktn.pkt_ansnr = anschrift.ansnr) " +
                    "AND (anschrift.ansnr = kunden.kdn_lfdnr) " +
                    "and kdn_typ = 'D' " +
                    "and kdn_kontonr in (40404, 19433, 45509, 46911, 48763)";

        OdbcCommand command = new OdbcCommand(getKunde, connection);
        DataTable finalTable = Lists.Find(_ => _.TableName == dataTable.TableName);
        dataTable = WriteTable(command, dataTable);

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
    rowKd[79] = "@";
    rowKd[80] = "@";
    rowKd[81] = "@";
    rowKd[82] = "@";
    rowKd[83] = "@";
}

if (kobensen.Rows.Contains(rowKd[0].ToString()))
{
    foreach (DataRow row in kobensen.Rows)
    {
        if (row[0].ToString() == rowKd[0].ToString())
        {
            rowKd[47] = RemoveWhiteSpace(row[2].ToString());
        }
    }
}

rowKd[0] = standort + rowKd[0].ToString();

rowKd[7] = rowKd[5].ToString().ToUpper();

try
{
    rowKd[34] = SetBrancheNr(rowKd[34].ToString());
}

catch (Exception e)
{
    Logger(e.Message);

    rowKd[34] = "@";
}

if (rowKd[18] != null)
{
    if (rowKd[4].ToString() != "FR")
    {
        try
        {
            GetHnr(rowKd[18].ToString());

            rowKd[18] = street;
            rowKd[20] = hNr;
        }
        catch (Exception e)
        {
            Logger(e.Message);
        }
    }
    else
    {
        rowKd[20] = "@";
    }
}

rowKd[18] = CorrectStrasse(rowKd[18].ToString());
rowKd[39] = "Ja";

try
{

    if (int.Parse(rowKd[43].ToString()) > 0)
    {
        rowKd[43] = DateConvert(rowKd[43].ToString());
    }

    else
    {
        rowKd[43] = "@";
    }
}
catch (Exception ex)
{
    Logger(ex.Message);
    rowKd[43] = "@";
}

if (rowKd[50] != null && rowKd[50].ToString() != "@")
{
    if (rowKd[50].ToString() != "0")
    {
        rowKd[50] = standort + rowKd[50].ToString();
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
        if (Dictionaries.VerbaendeDict.ContainsKey(konto))
        {
            rowKd[53] = Dictionaries.VerbaendeDict[konto];
        }
        else
        {
            rowKd[53] = "@";
        }
    }
}
else
{
    rowKd[53] = string.Empty;
}

if (rowKd[53].ToString() == "119516" | rowKd[53].ToString() == "138145" | rowKd[53].ToString() == "119433" | rowKd[53].ToString() == "119693")
{
    try
    {
        rowKd[57] = GetNrVerb(rowKd[12].ToString(), rowKd[13].ToString(), rowKd[53].ToString());
    }
    catch (Exception e)
    {
        Logger(e.Message);

        rowKd[57] = "";
    }
}

if (rowKd[4].ToString() == "DE")
{
    try
    {
        rowKd[21] = Dictionaries.BundeslandDict[rowKd[14].ToString()];
        rowKd[18] = CorrectStrasse(rowKd[18].ToString());
    }
    catch (Exception e)
    {
        Logger(e.Message);
        rowKd[21] = "";
    }
}

try
{
    rowKd[35] = GetRegion(rowKd[4].ToString(), rowKd[14].ToString());
}
catch (Exception e)
{
    Logger(e.Message);
    rowKd[35] = "@";
}

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

rowKd[47] = RemoveWhiteSpace(rowKd[47].ToString());

if (rowKd[47].ToString() != string.Empty && rowKd[47].ToString().Length > 3)
{
    if (!Dictionaries.LandUstidList.Contains(rowKd[47].ToString().Substring(0, 2)))
    {
        MatchCollection matches = Regex.Matches(rowKd[47].ToString(), @"\d+");
        rowKd[45] = matches[0].Value.ToString();
        rowKd[47] = "";
    }
}


if (rowKd[4].ToString() == "NL")
{
    rowKd[14] = SetPLZ(rowKd[14].ToString(), rowKd[5].ToString());
    rowKd[5] = SetOrtNL(rowKd[5].ToString());
}

if (rowKd[32].ToString() == "" | rowKd[32].ToString() == null)
{
    rowKd[32] = rowKd[5].ToString().ToUpper(); ;
}

if (standort == "2" && Dictionaries.ZbdNrHBS2SDL.ContainsKey(rowKd[68].ToString()))
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
{
    rowKd[49] = "Nein";
}
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

if (rowKd[76] != null)
{
    rowKd[76] = Dictionaries.VADict[rowKd[76].ToString()];
}


if (rowKd[84] != null)
{
    try
    {
        rowKd[84] = GetPreisliste(rowKd[84].ToString());
    }

    catch (KeyNotFoundException knfe)
    {
        rowKd[84] = "@";
    }
}

if (rowKd[85] != null)
{
    try
    {
        rowKd[85] = GetKundenrabattgruppe(rowKd[85].ToString());
    }
    catch (KeyNotFoundException knfe)
    {
        rowKd[85] = "@";
    }
}

rowKd[86] = "Ja";
                    }
                    catch (Exception e)
{
    Logger(e.Message);
}
                }
            }
            else
            {
                
            string selectCommand = "SELECT " +
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
                "'@' AS 'Verteileruppe'," +
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
                "persktn.pkt_kdn_lief_nr ," +
                "'', " +          //48
                "'0'," +
                "anschrift.ans_ustid, " +         //50
                "'@' AS 'Rechnungsintervall', " +
                "kunden.kdn_faktkz, " +
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
                "kdn_c_ohneklm, " +                             //73
                "'2' AS 'Mahnverfahren', " +                    //74
                "'@' AS Expr39, " +                             //75
                "'@' AS Expr40, " +                             //76
                "'@' AS Expr41, " +                             //77
                "kunden.kdn_vsanr, " +                          //78
                "kdn_lbdnr," +                                        //79
                "'@' as 'Lieferrestriktion',  " +               //80    
                "'' as 'Vertrags'," +                                          //81
                "kunden.kdn_kredvers, " +                       //82
                "'@' AS 'KredLimit WKV', " +
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
                "'@' " +           //145
                "FROM kunden, anschrift,persktn " +
                "WHERE (persktn.pkt_ansnr = anschrift.ansnr) " +
                "AND (anschrift.ansnr = kunden.kdn_lfdnr) " +
                //"and kdn_typ = 'D' " +
                "and kdn_aenderung > 20240401";
        
            OdbcCommand adapterKunde = new OdbcCommand(selectCommand, connection);
            List<DataTable> dataTables = new List<DataTable>();
            dataTable = new DataTable("s_Kunde9");
            dataTable = WriteTable(adapterKunde, dataTable);
        
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
                                rowKd[82] = row[1];
                                rowKd[83] = row[4].ToString();
                                rowKd[84] = "B";
                                rowKd[85] = row[5];
                            }
                        }
                    }
        
                    else
                    {
                        rowKd[81] = "@";
                        rowKd[82] = "@";
                        rowKd[83] = "@";
                        rowKd[84] = "@";
                        rowKd[85] = "@";
                    }
        
                    if (kobensen.Rows.Contains(rowKd[0].ToString()))
                    {
                        foreach (DataRow row in kobensen.Rows)
                        {
                            if (row[0].ToString() == rowKd[0].ToString())
                            {
                                rowKd[50] = RemoveWhiteSpace(row[2].ToString());
                            }
                        }
                    }
        
                    ListRgZahler.Add(rowKd[53].ToString());
                    ListKdNummern.Add(rowKd[0].ToString());
                    rowKd[0] = standort + rowKd[0].ToString();
        
                    rowKd[37] = SetBrancheNr(rowKd[37].ToString());
        
        
                    if (rowKd[3].ToString() == "NL")
                    {
                        rowKd[13] = SetPLZ(rowKd[13].ToString(), rowKd[4].ToString());
                        rowKd[4] = SetOrtNL(rowKd[4].ToString());
                    }
                    rowKd[6] = rowKd[4].ToString().ToUpper();
                    if (rowKd[17] != null)
                    {
                        if (rowKd[3].ToString() != "FR")
                        {
                            try
                            {
                                GetHnr(rowKd[17].ToString());
        
                                rowKd[17] = street;
                                rowKd[19] = hNr;
                            }
                            catch (Exception e)
                            {
                                Logger(e.Message);
                            }
                        }
                        else
                        {
                            rowKd[19] = "@";
                        }
                    }
        
                    rowKd[17] = CorrectStrasse(rowKd[17].ToString());
        
                    if (int.Parse(rowKd[46].ToString()) > 0)
                    {
                        rowKd[46] = DateConvert(rowKd[46].ToString());
                    }
                    else
                    {
                        rowKd[46] = "@";
                    }
                }
        
                catch (Exception ex)
                {
                    Logger(ex.Message);
        
                    rowKd[46] = "";
                }
        
                if (rowKd[53] != null && rowKd[53].ToString() != "@")
                {
                    if (rowKd[53].ToString() != "0")
                    {
                        rowKd[53] = standort + rowKd[53].ToString();
                    }
        
                    else
                    {
                        rowKd[53] = "";
                    }
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
                            rowKd[56] = "@";
                        }
                    }
                }
                else
                {
                    rowKd[56] = string.Empty;
                }
        
        
                if (rowKd[56].ToString() == "119516" | rowKd[56].ToString() == "138145" | rowKd[56].ToString() == "119433" | rowKd[56].ToString() == "119693")
                {
                    rowKd[60] = GetNrVerb(rowKd[11].ToString(), rowKd[12].ToString(), rowKd[56].ToString());
                }
        
                if (rowKd[3].ToString().ToUpper() == "DE")
                {
                    try
                    {
                        rowKd[20] = Dictionaries.BundeslandDict[rowKd[13].ToString()];
                        rowKd[17] = CorrectStrasse(rowKd[17].ToString());
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
        
                rowKd[38] = GetRegion(rowKd[3].ToString(), rowKd[13].ToString());
        
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
        
                rowKd[50] = RemoveWhiteSpace(rowKd[50].ToString());
        
                if (rowKd[50].ToString() != string.Empty && rowKd[50].ToString().Length > 3)
                {
                    if (!Dictionaries.LandUstidList.Contains(rowKd[50].ToString().Substring(0, 2)) && rowKd[50].ToString().Length < 21)
                    {
                            rowKd[50] = string.Empty;   
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
        
                if (standort == "2" && Dictionaries.ZbdNrHBS2SDL.ContainsKey(rowKd[71].ToString()))
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
                    rowKd[73] = "Nein";
                }
                if (rowKd[78] != null && Dictionaries.VADict.ContainsKey(rowKd[78].ToString()))
                {
                    rowKd[78] = Dictionaries.VADict[rowKd[78].ToString()];
                }
        
                else
                {
                    rowKd[78] = "@";
                }
        
                if (rowKd[86] != null)
                {
                    try
                    {
                        rowKd[86] = GetPreisliste(rowKd[84].ToString());
                    }
        
                    catch (KeyNotFoundException knfe)
                    {
                        rowKd[86] = "@";
                    }
        
                    rowKd[86] = "";
                }
        
                if (rowKd[87] != null)
                {
                    try
                    {
                        rowKd[87] = string.Empty;
                    }
        
                    catch (KeyNotFoundException knfe)
                    {
                        rowKd[88] = "";
                    }
                }
                rowKd[88] = string.Empty;
                rowKd[89] = "Ja";
                rowKd[91] = "@";
        
                if (Betriebskalender.ContainsKey(rowKd[20].ToString()))
                {
                    rowKd[144] = Betriebskalender[rowKd[20].ToString()];
                }
            }
        }

            dataTable.TableName = "KundenFibu";
            return dataTable;
        }
        */
    }

}


    








