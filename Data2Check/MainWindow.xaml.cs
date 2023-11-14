using System;
using System.Data;
using System.Data.Odbc;
using System.IO;
using System.Net.NetworkInformation;
using System.ServiceProcess;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
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
        

        // String TextStatus
        static TextBlock txtStatus { get; set; }

        public MainWindow()
        {
            InitializeComponent();
            Task.Run(() => ExportData());
            SetRegistryKey();
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

        async Task<bool> IsJuniperAndConnectedAsync()
        {
            bool isJuniperConnected = false;

            while (!isJuniperConnected)
            {
                foreach (var networkInterface in NetworkInterface.GetAllNetworkInterfaces())
                {
                    if (networkInterface.Description.Contains("Juniper Network") && networkInterface.OperationalStatus == OperationalStatus.Up)
                    {
                        isJuniperConnected = true;
                        break; // Die Schleife beenden, wenn eine passende Verbindung gefunden wurde
                    }
                }

                if (!isJuniperConnected)
                {
                    await Task.Delay(30000); // Asynchron warten und erneut versuchen
                }
            }

            return isJuniperConnected;
        }
        async Task<bool> CheckOdbcAsync(OdbcConnection connection)
        {
            bool isOdbcConnected = false;

            while (!isOdbcConnected)
            {
                try
                {
                    await connection.OpenAsync(); // Asynchron versuchen, die Verbindung zu öffnen
                    isOdbcConnected = true;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Fehler beim Öffnen der ODBC-Verbindung: {ex.Message}");
                    await Task.Delay(30000); // Asynchron warten und erneut versuchen
                }
                finally
                {
                    connection.Close(); // Die Verbindung schließen, unabhängig davon, ob sie erfolgreich geöffnet wurde oder nicht
                }
            }

            return isOdbcConnected;
        }
        [STAThread]
        private async void ExportData()
        {
            await IsJuniperAndConnectedAsync();
            CancellationToken token = cancellation.Token;
            Operations operations = new Operations();
            SQLMethods queriesMethods = new SQLMethods();
            operations.FillAtradius(Atradius);
            Timer queryTimeoutTimer = new Timer(QueryTimeoutCallback, cancellation, 600000, Timeout.Infinite);

            foreach (OdbcConnection connection in Connections)
            {
                while (!await CheckOdbcAsync(connection))
                {
                    await Task.Delay(500); 
                }

                using (OdbcConnection odbc = connection)
                {
                    odbc.Open();
                    SetPreString(odbc.ConnectionString);
                    UpdateProgressBar(10);
                    //DataTable kunden = queriesMethods.GetKunden(operations.GetLastDate(), odbc, Standort.ToString());
                    //queriesMethods.Table2CSV(kunden, Path, preString);
                    //SetText("DU_Kunden_neu exportiert \n"); 
                    UpdateProgressBar(10);
                    //DataTable lieferanten = queriesMethods.GetLieferanten(operations.GetLastDate(), odbc, Standort.ToString());
                    //queriesMethods.Table2CSV(lieferanten, Path, preString);
                    //SetText("DU_Lieferanten_neu exportiert \n");
                    UpdateProgressBar(10);
                    DataTable kundenASCII = queriesMethods.GetKundenASCII(queriesMethods.GetDataKunden(Standort.ToString()), odbc);
                    queriesMethods.Table2CSV(kundenASCII, ASCIIPath, preString);
                    SetText("Kunden ASCII exportiert \n");
                    UpdateProgressBar(10);
                    DataTable lieferantenASCII = queriesMethods.GetLieferantenASCII(queriesMethods.GetDataLieferanten(Standort.ToString()), odbc);
                    queriesMethods.Table2CSV(lieferantenASCII, ASCIIPath, preString);
                    SetText("Lieferanten ASCII exportiert \n");
                    UpdateProgressBar(10);
                    DataTable belegeASCII = queriesMethods.GetBelegeASCII(odbc);
                    queriesMethods.Table2CSV(belegeASCII, ASCIIPath, preString);
                    SetText("Belege ASCII exportiert \n");
                }
                
                UpdateProgressBar(10);
                UpdateProgressBar(10);
                Standort++;
            }
            
            operations.SetLastDate(DateFile);                                                                  
        }

        // Methode zum Anzeigen des Status Text
        private void SetText(string text)
        {
           // Application.Current.Dispatcher.Invoke(() =>
           // {
           //     txtStatus.Text += text;
           //     txtStatus.BringIntoView();
           // });
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
            Application.Current.Dispatcher.Invoke(() =>
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
                MessageBox.Show("Fehler : " + ex.Message);
            }
        }

        private void ButtonCreateShortcut_Click(object sender, RoutedEventArgs e)
        {
            CreateShortcutInAutostart();
        }

        private void BtnStartClick(object sender, RoutedEventArgs e)
        {
            ExportData();
        }

        private void ButtonOptions_Click(object sender, RoutedEventArgs e)
        {
            
                 
        }
    }
}

