using System;
using System.Data;
using System.Data.Odbc;
using System.IO;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;

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

        public MainWindow()
        {
            InitializeComponent();
            Task.Run(() => ExportData());
            this.txtStatus.Text = " F E R T I G ";
            this.txtStatus.BringIntoView();

        }

        string GetLastDate()
        {
            string dateString = string.Empty;

            using (FileStream fstream = new FileStream(@"LastDate.txt", FileMode.Open, FileAccess.ReadWrite))
            {
                using (StreamReader sreader = new StreamReader(fstream))
                {
                    sreader.BaseStream.Position = 0;
                    dateString = sreader.ReadLine();
                }
            }

            return dateString;
        }

        [STAThread]
        private void ExportData()
        {
            
            Operations operations = new Operations();
            SQLMethods queriesMethods = new SQLMethods();

            operations.FillAtradius(Atradius);

            foreach (OdbcConnection connection in Connections)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                }
                
                connection.Open();
                UpdateProgressBar(10);
                DataTable kunden = queriesMethods.GetKunden(operations.GetLastDate(), connection, Standort.ToString());
                UpdateProgressBar(10);
                DataTable lieferanten = queriesMethods.GetLieferanten(operations.GetLastDate(), connection, Standort.ToString());
                UpdateProgressBar(10);
                SetPreString(connection.ConnectionString);
                UpdateProgressBar(10);
                queriesMethods.Table2CSV(kunden, Path, preString);
                UpdateProgressBar(10);
                queriesMethods.Table2CSV(lieferanten, Path, preString);
                UpdateProgressBar(10);
                Standort++;
                UpdateProgressBar(10);
                
             }
            operations.SetLastDate();
            
        }

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
        
     
        private void UpdateProgressBar(int value)
        {
            Application.Current.Dispatcher.Invoke(() =>
            {
                progressbar.Value += value;
                progressbar.BringIntoView();
            });
        }

        private void CreateShortcutInAutostart()
        {
            try
            {
                // Get the startup folder path
                string startupFolderPath = Environment.GetFolderPath(Environment.SpecialFolder.Startup);

                // Get your application's executable file
                FileInfo appFileInfo = new FileInfo(System.Reflection.Assembly.GetExecutingAssembly().Location);

                // Create a FileInfo for the shortcut's location (e.g., in the startup folder)
                FileInfo shortcutFileInfo = new FileInfo(startupFolderPath+"\\Data2Check.lnk");

                // Check if the shortcut file already exists, and delete it if needed
                if (shortcutFileInfo.Exists)
                {
                    shortcutFileInfo.Delete();
                }

                // Create the shortcut
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
                MessageBox.Show("Error creating shortcut: " + ex.Message);
            }
        }

        private void ButtonCreateShortcut_Click(object sender, RoutedEventArgs e)
        {
            CreateShortcutInAutostart();
        }
    }
}

