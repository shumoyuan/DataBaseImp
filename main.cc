#include "Database.h"

using namespace std;

int main () {
    
    string option;
    int state = 0; // 0 for shut down, 1 from fire up;
    
    Database db;
    
    while (true) {
        cout << endl << "Please select your operation:" << endl << endl;
        cout << "1. Create a new database (will delete current database)" << endl;
        cout << "2. Start current database" << endl;
        cout << "3. Execute SQL" << endl;
        cout << "4. Shut down database" << endl;
        cout << "5. Exit" << endl << endl;
        
        cout << "Your choice: ";
        cin >> option;
        
        if (option == "1") {
            if (state != 0) cerr << "Error: Must shut down current database before create a new one" << endl;
            else {
                db.Create();
                cout << "Info: New database has been created." << endl;
                cout << "++++++++++++++++++++++++++++++++++++++++" << endl;
            }
        }
        else if (option == "2") {
            if (state != 0) cerr << "Error: Database has already been fired up!" << endl;
            else {
                db.Open();
                state = 1;
                cout << "Info: Database has been fired up." << endl;
                cout << "++++++++++++++++++++++++++++++++++++++++" << endl;
            }
        }
        else if (option == "3") {
            if (state != 1) cerr << "Error: Database hasn't been fired up yet!" << endl;
            else {
                db.Execute();
                cout << "++++++++++++++++++++++++++++++++++++++++" << endl;
            }
        }
        else if (option == "4") {
            if (state != 1) cerr << "Error: Database hasn't been fired up yet!" << endl;
            else {
                db.Close();
                state = 0;
                cout << "Info: Database has been shut down." << endl;
                cout << "++++++++++++++++++++++++++++++++++++++++" << endl;
            }
        }
        else if (option == "5") {
            if (state == 1) cerr << "Error: Shut down database first!" << endl;
            else {
                cout << "Good bye~" << endl;
                return 0;
            }
        }
        else {
            cerr << "Invalid option! Valid option must be 1~5. Please input again!" << endl;
        }
    }
}


