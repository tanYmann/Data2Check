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
        private static readonly DateTime dateTime = new DateTime();
        public DateTime Endtime = dateTime;
        static string LogFile = @"\\bauer-gmbh.org\DFS\SL\PROALPHA\10. Team-DÜ, EDI, WF\Datenexport\Operations.Logger.txt";
        //static bool timerInitialized = false;
        Window1 Window1 = new Window1();
        
        //asynchrone Mainmethode
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
            this.Endtime = SetEndTime(new TimeSpan(0, 2, 0));
            InitializeComponent();
            InitializeNotifyIcon();
            SetRegistryKey();
            _ = Task.Run(async () => await ExportData());
        }

        //Schreiben in den Logfile
        void WriteLogFile(string line)
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

        //  Get und Set LastDate
        (string Date2End,string Date2Start) GetLastDate()
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


            DateTime dateTimeStart = DateTime.Now.AddDays(-4);

            dateDay = string.Empty;
            dateMonth = string.Empty;

            if (dateTimeStart.Day < 10) 
            {
               dateDay = "0"+dateTimeStart.Day.ToString();
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



            date2Start = dateTimeStart.Year.ToString()+dateMonth+dateDay;

            return (date2End, date2Start);
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

                DataTable dataTable = new DataTable();
      
                if (token.IsCancellationRequested)
                {
                    return;
                }

                foreach (var key in dict.Keys)
                {
                    string query = string.Empty;
                    dict.TryGetValue(key, out query);
                    query = string.Format(dict[key.ToString()].ToString(), GetLastDate().Date2Start, GetLastDate().Date2End);
                    using (OdbcCommand command = new OdbcCommand(query, conn))
                    {
                        if (conn.State != ConnectionState.Open)
                        {
                            conn.Open();
                        }

                        dataTable.Load(command.ExecuteReader());
                        foreach (DataColumn col in dataTable.Columns) { col.ReadOnly = false; }

                        if (key.ToString() == "DU_Kunde")
                        {
                            

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
                                                rowKd[50] = operations.RemoveWhiteSpace(row[2].ToString());
                                            }
                                        }
                                    }


                                    rowKd[0] = Standort + rowKd[0].ToString();

                                    rowKd[37] = operations.SetBrancheNr(rowKd[37].ToString());


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
                                            try
                                            {

                                                rowKd[17] = operations.GetHnr(rowKd[17].ToString()).street;
                                                rowKd[19] = operations.GetHnr(rowKd[17].ToString()).houseNumber;
                                            }
                                            catch (Exception e)
                                            {
                                                operations.Logger(e.Message);
                                            }
                                        }
                                        else
                                        {
                                            rowKd[19] = "@";
                                        }
                                    }

                                    rowKd[17] = operations.CorrectStrasse(rowKd[17].ToString());

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
                                        rowKd[86] = operations.GetPreisliste(rowKd[84].ToString());
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

                                if (Dictionaries.Betriebskalender.ContainsKey(rowKd[20].ToString()))
                                {
                                    rowKd[144] = Dictionaries.Betriebskalender[rowKd[20].ToString()];
                                }

                                if (Dictionaries.VerbandList.Contains(rowKd[56].ToString()))
                                {
                                    rowKd[58] = "JA";
                                }
                                else
                                {
                                    rowKd[58] = "Nein";
                                }

                            }

                            queriesMethods.Table2CSV(dataTable, ASCIIPath, preString);
                        }
                    }
                    if (key.ToString() == "DU_Lieferant")
                    {

                        query = string.Format(dict[key.ToString()].ToString(), GetLastDate().Date2Start, GetLastDate().Date2End);

                        DataTable kobensen = new DataTable();
                        dataTable = new DataTable("DU_Lieferant");
                        OdbcCommand cmd = new OdbcCommand(query, conn);
                        dataTable = operations.WriteTable(cmd, dataTable);

                        foreach (DataRow row in dataTable.Rows)
                        {
                            if (Standort.ToString() == "2")
                            {
                                if (dictionaries.HBSLief2Change.ContainsKey(row[0].ToString()))
                                {
                                    row[0] = dictionaries.HBSLief2Change[row[0].ToString()];
                                }
                            }
                            if (Dictionaries.LieferantenAvisList.Contains(row[0].ToString()))
                            {
                                row[47] = "0";
                            }

                            if (row[0].ToString().Length < 6)
                            {
                                row[0] = row[0].ToString() + Standort.ToString();
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
                                    operations.Logger(e.Message);
                                }
                            }

                            try
                            {
                                if (row[3].ToString() != "FR")
                                {
                                    operations.GetHnr(row[17].ToString());
                                    row[17] = operations.GetHnr(row[17].ToString()).street;
                                    row[19] = operations.GetHnr(row[17].ToString()).houseNumber;
                                }

                                else
                                {
                                    row[19] = "@";
                                }

                                row[37] = operations.SetBrancheNr(row[37].ToString());
                                row[38] = Dictionaries.Betriebskalender[row[20].ToString()];
                            }
                            catch (Exception e)
                            {
                                operations.Logger(e.Message);
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
                                if (Standort.ToString() == "1")
                                {
                                    row[48] = Dictionaries.ZbdNrVK[row[48].ToString()];
                                }
                                if (Standort.ToString() == "2")
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

                        queriesMethods.Table2CSV(dataTable, ASCIIPath, preString);
                    }



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

                    UpdateProgressBar(10);
                    UpdateProgressBar(10);
                

                    System.Windows.Application.Current.Dispatcher.Invoke(() =>
                    {
                        progressbar.Value = 0;
                        progressbar.BringIntoView();
                        InitializeTimer();
                    });
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

        // Vergangene Zeit
        async void TimerElapsed(object sender, ElapsedEventArgs e)
        {
            Operations operations = new Operations();   
            TimeSpan remainingTime = NextExecution();
            System.Windows.Application.Current.Dispatcher.Invoke(() =>
            SetRemainingTime(remainingTime));

            if (remainingTime <= TimeSpan.Zero)
            {
                timer.Stop();
                await System.Windows.Application.Current.Dispatcher.InvokeAsync(() =>
                {
                    Task.Run(async () => await ExportData());

                    string dateWrite = GetLastDate().Date2End.ToString();
                    using (FileStream fstream = new FileStream(DateFile, FileMode.Open, FileAccess.ReadWrite))
                    using (StreamWriter writer = new StreamWriter(fstream))
                    {
                        writer.BaseStream.Position = 0;
                        writer.WriteLine(dateWrite);
                    }


                });

            }
        }

        // Anzeige der verbleibenden Zeit bis zur nächsten Ausführung
        void SetRemainingTime(TimeSpan remainingTime)
        {
            txtRemainingTime.Text = $"Zeit bis zum nächsten Export: {remainingTime.Hours}h {remainingTime.Minutes}m {remainingTime.Seconds}s";
        }

        // Zeitspanne bis zur nächsten Ausführung
        public TimeSpan NextExecution()
        {
            DateTime now = DateTime.Now;
            //DateTime targetDateTime = now.AddMinutes(30);
            DateTime targetDateTime = new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.Now.Day,18, 30, 0);
            targetDateTime += TimeSpan.FromDays(1);
            
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

            TimeSpan span = targetDateTime - now;


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
            _ = Task.Run(async () => await ExportData());
        }

        // Button zum Öffnen der Optionen
        void ButtonOptions_Click(object sender, RoutedEventArgs e)
        {
            if (Window1.WindowState != WindowState.Maximized)
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
    }
}
      