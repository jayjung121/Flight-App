import java.io.FileInputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Map;
/**
 * Runs queries against a back-end database
 */
public class Query
{
  private String configFilename;
  private Properties configProps = new Properties();

  private String jSQLDriver;
  private String jSQLUrl;
  private String jSQLUser;
  private String jSQLPassword;

  // DB Connection
  private Connection conn;

  // Logged In User
  private String username; // customer username is unique
  private Boolean loggedIn = false;
  // Canned queries
  //private Map<Integer, ArrayList<String>> itineraries;
  private ArrayList<ArrayList<Integer>> itineraries;





  private static final String CHECK_FLIGHT_CAPACITY = "SELECT capacity FROM Flights WHERE fid = ?";
  private PreparedStatement checkFlightCapacityStatement;

  // transactions
  private static final String BEGIN_TRANSACTION_SQL = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;";
  private PreparedStatement beginTransactionStatement;

  private static final String COMMIT_SQL = "COMMIT TRANSACTION";
  private PreparedStatement commitTransactionStatement;

  private static final String ROLLBACK_SQL = "ROLLBACK TRANSACTION";
  private PreparedStatement rollbackTransactionStatement;

// Transactions by Jay

//update user's balance
private static final String UPDATE_BALANCE = "UPDATE Users SET balance = ? WHERE username = ?";
private PreparedStatement updateBalancestatement;

//update reservation Paid
private static final String UPDATE_PAID = "UPDATE Reservations SET paid = 1 WHERE reservation_id = ?";
private PreparedStatement updatePaid;

private static final String UPDATE_USER_BALANCE = "UPDATE Users SET balance = ? WHERE username = ?";
private PreparedStatement updateUserBalance;

//user money check
private static final String CHECK_MONEY = "SELECT balance " + "FROM Users " + "WHERE username = ?";
private PreparedStatement checkBalanceStatement;


private static final String FIND_FLIGHT_SQL =
    "SELECT day_of_month,carrier_id,flight_num,origin_city,dest_city,actual_time,fid, capacity, price "
        + "FROM Flights "
        + "WHERE fid = ? "
        + "ORDER BY actual_time ASC";
private PreparedStatement flightStatement;
// get reservationId
private static final String CHECK_RESERVATION =
    "SELECT username, date, reservation_id, fid, paid " +
    "FROM Reservations WHERE username = ? " +
    "ORDER BY reservation_id ASC";
private PreparedStatement checkReservationStatement;

private static final String TWO_RESERVATIONS =
  "SELECT fid FROM Reservations WHERE reservation_id = ?";
private PreparedStatement checkTwoHopReservation;



private static final String GET_RESERVATION_ID = "SELECT id FROM ReservationIDs WHERE condition = ?";
private PreparedStatement getReservationIdStatement;


// Make reservation
private static final String RESERVATION_SQL =
    "INSERT INTO Reservations(reservation_id,username,date, fid) "
        + "VALUES(?, ?, ?, ?)";
private PreparedStatement reserveStatement;

private static final String INCREMENT_RESERVATION_ID =
"UPDATE ReservationIDs SET id = id + 1";

private PreparedStatement incrementIdStatement ;

// check booking passanger account
private static final String CHECK_BOOKING_SQL =
    "SELECT passengers_count "
        + "FROM Booking "
        + "WHERE flight_id = ?";
private PreparedStatement checkBookingStatement;


// check day for booking to see if there exist two booking on the same date
private static final String CHECK_DAY =
  "SELECT DISTINCT day_of_month " +
  "FROM Reservations " +
  "WHERE username = ?";
private PreparedStatement checkDay;

// Add to booking table
private static final String ADD_BOOKING =
"INSERT INTO Booking (flight_id, passengers_count) " +
"VALUES (?,1)";
private PreparedStatement addBookingStatement;
// update booking
private static final String UPDATE_BOOKING_SQL =
    "UPDATE Booking "
        + "SET passengers_count = ? "
        + "WHERE flight_id = ?";
private PreparedStatement updateBookingStatement;

// private static final String CHECK_FLIGHT_SQL =
//     "SELECT passengers_count "
//         + "FROM Booking "
//         + "WHERE flight_id = ?";
// private PreparedStatement checkFlightStatement;

// Search one hop flight
private static final String SEARCH_ONE_HOP_SQL =
    "SELECT TOP (?) fid,actual_time,day_of_month,carrier_id,flight_num,origin_city,dest_city,capacity,price "
        + "FROM Flights "
        + "WHERE origin_city = ? AND dest_city = ? AND day_of_month = ? "
        + "AND canceled = 0 "
        + "ORDER BY actual_time ASC, fid ASC";
private PreparedStatement searchOneHopStatement;


// Search two hop flights
private static final String SEARCH_TWO_HOP_SQL =
    "SELECT TOP (?) F.fid AS Ffid ,F.day_of_month AS Fday_of_month,F.carrier_id AS Fcarrier_id,F.flight_num AS Fflight_num,F.origin_city AS Forigin_city,F.dest_city AS Fdest_city,F.actual_time AS Factual_time, F.capacity AS Fcapacity, F.price AS Fprice, "
        + "S.fid AS Sfid, S.day_of_month AS Sday_of_month,S.carrier_id AS Scarrier_id,S.flight_num AS Sflight_num,S.origin_city AS Sorigin_city,S.dest_city AS Sdest_city, S.actual_time AS Sactual_time, S.capacity AS Scapacity, S.price AS Sprice, "
        + "(F.actual_time + S.actual_time) AS total_time "
        + "FROM Flights AS F "
        + "INNER JOIN Flights AS S "
        + "ON S.origin_city = F.dest_city "
        + "AND F.origin_city = ? "
        + "AND S.dest_city = ? "
        + "AND F.day_of_month = ? "
        + "AND S.day_of_month = F.day_of_month "
        + "AND F.canceled = 0 "
        + "AND S.canceled = 0 "
        + "ORDER BY (F.actual_time + S.actual_time) ASC, F.fid ASC, S.fid ASC";
private PreparedStatement searchTwoHopStatement;

// Clear table
private static final String CLEAR_TABLES = "DELETE FROM Reservations";
private PreparedStatement clearTablesStatement;
private static final String CLEAR_TABLES1 = "DELETE FROM Booking";
private PreparedStatement clearTablesStatement1;
private static final String CLEAR_TABLES2 = "DELETE FROM ReservationIDs";
private PreparedStatement clearTablesStatement2;

// INsert new customer to Users table
private static final String CREATE_CUSTOMERS = "INSERT INTO Users(username, password, balance) VALUES (?, ?, ?)";
private PreparedStatement createCustomersStatement;
// Check customer
private static final String CHECK_CUSTOMERS = "SELECT username " + "FROM Users " + "WHERE username = ?";
private PreparedStatement checkCustomersStatement;
// Return username given username and password
private static final String LOGIN_SQL =
			"SELECT username "
					+ "FROM Users "
					+ "WHERE username = ? AND password = ?";
private PreparedStatement loginStatement;




// For isolation

private PreparedStatement setTransactionIsolation;








  class Flight
  {
    public int fid;
    public int dayOfMonth;
    public String carrierId;
    public String flightNum;
    public String originCity;
    public String destCity;
    public int time;
    public int capacity;
    public int price;

    // public Flight(int fid, int dayOfMonth, String carrierId,String flightNum,String originCity, String destCity, int time, int capacity, int price) {
    //   this.fid = fid;
    //   this.dayOfMonth = dayOfMonth;
    //   this.carrierId = carrierId;
    //   this.flightNum = flightNum;
    //   this.originCity = originCity;
    //   this.destCity = destCity;
    //   this.time = time;
    //   this.capacity = capacity;
    //   this.price = price;
    // }

    @Override
    public String toString()
    {
      return "ID: " + fid + " Day: " + dayOfMonth + " Carrier: " + carrierId +
              " Number: " + flightNum + " Origin: " + originCity + " Dest: " + destCity + " Duration: " + time +
              " Capacity: " + capacity + " Price: " + price;
    }
  }

  public Query(String configFilename)
  {
    this.configFilename = configFilename;
  }

  /* Connection code to SQL Azure.  */
  public void openConnection() throws Exception
  {
    configProps.load(new FileInputStream(configFilename));

    jSQLDriver = configProps.getProperty("flightservice.jdbc_driver");
    jSQLUrl = configProps.getProperty("flightservice.url");
    jSQLUser = configProps.getProperty("flightservice.sqlazure_username");
    jSQLPassword = configProps.getProperty("flightservice.sqlazure_password");

    /* load jdbc drivers */
    Class.forName(jSQLDriver).newInstance();

    /* open connections to the flights database */
    conn = DriverManager.getConnection(jSQLUrl, // database
            jSQLUser, // user
            jSQLPassword); // password

    conn.setAutoCommit(true); //by default automatically commit after each statement

    /* You will also want to appropriately set the transaction's isolation level through:
       conn.setTransactionIsolation(...)
       See Connection class' JavaDoc for details.
    */
    //SET TRANSACTION ISOLATION LEVEL SERIALIZABLE
    // TO BE DONE !!!!
  }

  public void closeConnection() throws Exception
  {
    conn.close();
  }

  /**
   * Clear the data in any custom tables created. Do not drop any tables and do not
   * clear the flights table. You should clear any tables you use to store reservations
   * and reset the next reservation ID to be 1.
   */

  public void clearTables ()
  {
    try {
      // sql statement for truncate???? Delete!!! table!!!
      clearTablesStatement.executeUpdate();
      clearTablesStatement1.executeUpdate();
      clearTablesStatement2.executeUpdate();

    } catch (SQLException e) {
      e.printStackTrace();
    }

  }

  /**
   * prepare all the SQL statements in this method.
   * "preparing" a statement is almost like compiling it.
   * Note that the parameters (with ?) are still not filled in
   */
  public void prepareStatements() throws Exception
  {
    beginTransactionStatement = conn.prepareStatement(BEGIN_TRANSACTION_SQL);
    commitTransactionStatement = conn.prepareStatement(COMMIT_SQL);
    rollbackTransactionStatement = conn.prepareStatement(ROLLBACK_SQL);

    checkFlightCapacityStatement = conn.prepareStatement(CHECK_FLIGHT_CAPACITY);

    /* add here more prepare statements for all the other queries you need */
    searchOneHopStatement = conn.prepareStatement(SEARCH_ONE_HOP_SQL);
    searchTwoHopStatement = conn.prepareStatement(SEARCH_TWO_HOP_SQL);
    clearTablesStatement = conn.prepareStatement(CLEAR_TABLES);
    clearTablesStatement1 = conn.prepareStatement(CLEAR_TABLES1);
    clearTablesStatement2 = conn.prepareStatement(CLEAR_TABLES2);
    createCustomersStatement = conn.prepareStatement(CREATE_CUSTOMERS);
    loginStatement = conn.prepareStatement(LOGIN_SQL);
    checkCustomersStatement = conn.prepareStatement(CHECK_CUSTOMERS);
    checkDay = conn.prepareStatement(CHECK_DAY);
    addBookingStatement = conn.prepareStatement(ADD_BOOKING);
    checkBookingStatement = conn.prepareStatement(CHECK_BOOKING_SQL);
    updateBookingStatement = conn.prepareStatement(UPDATE_BOOKING_SQL);
    getReservationIdStatement = conn.prepareStatement(GET_RESERVATION_ID);
    reserveStatement = conn.prepareStatement(RESERVATION_SQL);
    incrementIdStatement = conn.prepareStatement(INCREMENT_RESERVATION_ID);
    flightStatement = conn.prepareStatement(FIND_FLIGHT_SQL);
    checkReservationStatement = conn.prepareStatement(CHECK_RESERVATION);
    checkTwoHopReservation = conn.prepareStatement(TWO_RESERVATIONS);
    checkBalanceStatement = conn.prepareStatement(CHECK_MONEY);
    updatePaid = conn.prepareStatement(UPDATE_PAID);
    updateUserBalance = conn.prepareStatement(UPDATE_USER_BALANCE);
    updateBalancestatement = conn.prepareStatement(UPDATE_BALANCE);
    itineraries = new ArrayList<ArrayList<Integer>>();
  }

  /**
   * Takes a user's username and password and attempts to log the user in.
   *
   * @param username
   * @param password
   *
   * @return If someone has already logged in, then return "User already logged in\n"
   * For all other errors, return "Login failed\n".
   *
   * Otherwise, return "Logged in as [username]\n".
   */
   // login worked(o)!!
  public String transaction_login(String username, String password)
  {
    String result = "";
    try{
      if (loggedIn) {
        return("User already logged in\n");
      } else {
        loginStatement.clearParameters();
        loginStatement.setString(1, username);
        loginStatement.setString(2, password);
        ResultSet loginResults = loginStatement.executeQuery();
        if (loginResults.next()) {
          this.username = username;
          this.loggedIn = true;
          result = "Logged in as " + username + "\n";
        } else {
          result = "Login failed\n";
        }
        loginResults.close();
      }
    } catch (SQLException e) {
       e.printStackTrace();
    }
    // LOGIN!!
    return(result);
  }

  /**
   * Implement the create user function.
   *
   * @param username new user's username. User names are unique the system.
   * @param password new user's password.
   * @param initAmount initial amount to deposit into the user's account, should be >= 0 (failure otherwise).
   *
   * @return either "Created user {@code username}\n" or "Failed to create user\n" if failed.
   */
  public String transaction_createCustomer (String username, String password, int initAmount)
  {
    String result = "";
    try{
      createCustomersStatement.clearParameters();
      createCustomersStatement.setString(1, username);
      createCustomersStatement.setString(2, password);
      createCustomersStatement.setInt(3, initAmount);
      int createedOrNot = createCustomersStatement.executeUpdate();
      if (createedOrNot == 1) {
        result = "Created user " + username + "\n";
      } else {
        result = "Failed to create user\n";
      }
    } catch (SQLException e){
      e.printStackTrace();
    }
    return(result);
  }

  /**
   * Implement the search function.
   *
   * Searches for flights from the given origin city to the given destination
   * city, on the given day of the month. If {@code directFlight} is true, it only
   * searches for direct flights, otherwise is searches for direct flights
   * and flights with two "hops." Only searches for up to the number of
   * itineraries given by {@code numberOfItineraries}.
   *
   * The results are sorted based on total flight time.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight if true, then only search for direct flights, otherwise include indirect flights as well
   * @param dayOfMonth
   * @param numberOfItineraries number of itineraries to return
   *
   * @return If no itineraries were found, return "No flights match your selection\n".
   * If an error occurs, then return "Failed to search\n".
   *
   * Otherwise, the sorted itineraries printed in the following format:
   *
   * Itinerary [itinerary number]: [number of flights] flight(s), [total flight time] minutes\n
   * [first flight in itinerary]\n
   * ...
   * [last flight in itinerary]\n
   *
   * Each flight should be printed using the same format as in the {@code Flight} class. Itinerary numbers
   * in each search should always start from 0 and increase by 1.
   *
   * @see Flight#toString()
   */
  public String transaction_search(String originCity, String destinationCity, boolean directFlight, int dayOfMonth,
                                   int numberOfItineraries)
  {
    //return transaction_search_unsafe(originCity, destinationCity, directFlight, dayOfMonth, numberOfItineraries);
    return transaction_search_safe(originCity, destinationCity, directFlight, dayOfMonth, numberOfItineraries);

  }

  // My safe transaction_search
  //


  private String transaction_search_safe(String originCity, String destinationCity, boolean directFlight,
                                          int dayOfMonth, int numberOfItineraries)
  {
    StringBuffer sb = new StringBuffer();
    itineraries.clear();
    try
    { // direct = 1

      searchOneHopStatement.setInt(1, numberOfItineraries);
      searchOneHopStatement.setString(2, originCity);
      searchOneHopStatement.setString(3, destinationCity);
      searchOneHopStatement.setInt(4, dayOfMonth);
      ResultSet oneHopResults = searchOneHopStatement.executeQuery();
      int itinerary_num = 0;
      //int oneHopSize = 0;


      while (oneHopResults.next()) {
          int result_fid = oneHopResults.getInt("fid");
          int result_dayOfMonth = oneHopResults.getInt("day_of_month");
          String result_carrierId = oneHopResults.getString("carrier_id");
          String result_flightNum = oneHopResults.getString("flight_num");
          String result_originCity = oneHopResults.getString("origin_city");
          String result_destCity = oneHopResults.getString("dest_city");
          int result_time = oneHopResults.getInt("actual_time");
          int result_capacity = oneHopResults.getInt("capacity");
          int result_price = oneHopResults.getInt("price");
          String itinerary = "Itinerary " + itinerary_num + ": 1 flight(s), "+ result_time + " minutes"+ "\n";
          String flight = "ID: " + result_fid + " Day: " + result_dayOfMonth + " Carrier: " + result_carrierId + " Number: " + result_flightNum + " Origin: " + result_originCity + " Destination: " + result_destCity + " Duration: " + result_time + " Capacity: " + result_capacity + " Price: " + result_price + "\n";
          sb.append(itinerary);
          sb.append(flight);
          ArrayList<Integer> temp = new ArrayList<Integer>();
          temp.add(result_dayOfMonth);
          temp.add(result_fid);
          itineraries.add(temp);
          //temp.add(itinerary);
          //temp.add(flight);
          //itineraries.put(itinerary_num, temp);

          itinerary_num++;
      }
      oneHopResults.close();

      // TWO HOP!!
      if (directFlight == false && itinerary_num < numberOfItineraries) {
        // do twohop
        searchTwoHopStatement.setInt(1, numberOfItineraries);
        searchTwoHopStatement.setString(2, originCity);
        searchTwoHopStatement.setString(3, destinationCity);
        searchTwoHopStatement.setInt(4, dayOfMonth);
        ResultSet twoHopResults = searchTwoHopStatement.executeQuery();
        while (twoHopResults.next()) {
          if(itinerary_num != numberOfItineraries) {
            int result_total_time = twoHopResults.getInt("total_time");
            //1
            int result1_fid = twoHopResults.getInt("Ffid");
            int result1_dayOfMonth = twoHopResults.getInt("Fday_of_month");
            String result1_carrierId = twoHopResults.getString("Fcarrier_id");
            String result1_flightNum = twoHopResults.getString("Fflight_num");
            String result1_originCity = twoHopResults.getString("Forigin_city");
            String result1_destCity = twoHopResults.getString("Fdest_city");
            int result1_time = twoHopResults.getInt("Factual_time");
            int result1_capacity = twoHopResults.getInt("Fcapacity");
            int result1_price = twoHopResults.getInt("Fprice");
            //2
            int result2_fid = twoHopResults.getInt("Sfid");
            int result2_dayOfMonth = twoHopResults.getInt("Sday_of_month");
            String result2_carrierId = twoHopResults.getString("Scarrier_id");
            String result2_flightNum = twoHopResults.getString("Sflight_num");
            String result2_originCity = twoHopResults.getString("Sorigin_city");
            String result2_destCity = twoHopResults.getString("Sdest_city");
            int result2_time = twoHopResults.getInt("Sactual_time");
            int result2_capacity = twoHopResults.getInt("Scapacity");
            int result2_price = twoHopResults.getInt("Sprice");
            String itinerary2 = "Itinerary " + itinerary_num + ": 2 flight(s), "+ result_total_time + " minutes"+ "\n";
            String flight1 = "ID: " + result1_fid + " Day: " + result1_dayOfMonth + " Carrier: " + result1_carrierId + " Number: " + result1_flightNum +
            " Origin: " + result1_originCity + " Destination: " + result1_destCity + " Duration: " + result1_time + " Capacity: " + result1_capacity +
            " Price: " + result1_price + "\n";
            String flight2 = "ID: " + result2_fid + " Day: " + result2_dayOfMonth + " Carrier: " + result2_carrierId + " Number: " + result2_flightNum +
            " Origin: " + result2_originCity + " Destination: " + result2_destCity + " Duration: " + result2_time + " Capacity: " + result2_capacity +
            " Price: " + result2_price + "\n";
            sb.append(itinerary2);
            sb.append(flight1);
            sb.append(flight2);

            ArrayList<Integer> temp2 = new ArrayList<Integer>();
            temp2.add(result1_dayOfMonth);
            temp2.add(result1_fid);
            temp2.add(result1_dayOfMonth);
            temp2.add(result2_fid);
            //temp.add(itinerary2);
            //temp.add(flight1);
            //temp.add(flight2);
            //itineraries.put(itinerary_num, temp);
            itinerary_num++;
          }
          twoHopResults.close();


        }

      }


    } catch (SQLException e) { e.printStackTrace(); }

    return sb.toString();
  }
  /**
   * Same as {@code transaction_search} except that it only performs single hop search and
   * do it in an unsafe manner.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight
   * @param dayOfMonth
   * @param numberOfItineraries
   *
   * @return The search results. Note that this implementation *does not conform* to the format required by
   * {@code transaction_search}.
   */
  // private String transaction_search_unsafe(String originCity, String destinationCity, boolean directFlight,
  //                                         int dayOfMonth, int numberOfItineraries)
  // {
  //   StringBuffer sb = new StringBuffer();
  //
  //   try
  //   {
  //     // one hop itineraries
  //     String unsafeSearchSQL =
  //             "SELECT TOP (" + numberOfItineraries + ") day_of_month,carrier_id,flight_num,origin_city,dest_city,actual_time,capacity,price "
  //                     + "FROM Flights "
  //                     + "WHERE origin_city = \'" + originCity + "\' AND dest_city = \'" + destinationCity + "\' AND day_of_month =  " + dayOfMonth + " "
  //                     + "ORDER BY actual_time ASC";
  //
  //     Statement searchStatement = conn.createStatement();
  //     ResultSet oneHopResults = searchStatement.executeQuery(unsafeSearchSQL);
  //
  //     while (oneHopResults.next())
  //     {
  //       int result_dayOfMonth = oneHopResults.getInt("day_of_month");
  //       String result_carrierId = oneHopResults.getString("carrier_id");
  //       String result_flightNum = oneHopResults.getString("flight_num");
  //       String result_originCity = oneHopResults.getString("origin_city");
  //       String result_destCity = oneHopResults.getString("dest_city");
  //       int result_time = oneHopResults.getInt("actual_time");
  //       int result_capacity = oneHopResults.getInt("capacity");
  //       int result_price = oneHopResults.getInt("price");
  //
  //       sb.append("Day: " + result_dayOfMonth + " Carrier: " + result_carrierId + " Number: " + result_flightNum + " Origin: " + result_originCity + " Destination: " + result_destCity + " Duration: " + result_time + " Capacity: " + result_capacity + " Price: " + result_price + "\n");
  //     }
  //     oneHopResults.close();
  //   } catch (SQLException e) { e.printStackTrace(); }
  //
  //   return sb.toString();
  // }

  /**
   * Implements the book itinerary function.
   *
   * @param itineraryId ID of the itinerary to book. This must be one that is returned by search in the current session.
   *
   * @return If the user is not logged in, then return "Cannot book reservations, not logged in\n".
   * If try to book an itinerary with invalid ID, then return "No such itinerary {@code itineraryId}\n".
   * If the user already has a reservation on the same day as the one that they are trying to book now, then return
   * "You cannot book two flights in the same day\n".
   * For all other errors, return "Booking failed\n".
   *
   * And if booking succeeded, return "Booked flight(s), reservation ID: [reservationId]\n" where
   * reservationId is a unique number in the reservation system that starts from 1 and increments by 1 each time a
   * successful reservation is made by any user in the system.
   */
  public String transaction_book(int itineraryId)
  {
    String returnStatement = "Booking failed\n";
    if(username == null) {
      return ("Cannot book reservations, not logged in\n");
    } else if (itineraryId < 0 || itineraryId > itineraries.size()) {
      return("No such itinerary "+ itineraryId  +"\n");
    }
    try {
    beginTransaction();
    // got itinerary
    //System.out.println(itineraries);
    ArrayList<Integer> itinerary = itineraries.get(itineraryId);
    //System.out.println(itinerary);
    // for all flight in itinerary
    for (int i = 1; i * 2 <= itinerary.size(); i++) {
      // get fid
      int date = itinerary.get(i * 2 - 2);
      int fid = itinerary.get(i * 2 -1);

      // get passengers_count
      checkBookingStatement.clearParameters();
      checkBookingStatement.setInt(1, fid);
      ResultSet resultPassengerCount = checkBookingStatement.executeQuery();

      if(!resultPassengerCount.next()) {
        // new booking
        addBookingStatement.clearParameters();
        addBookingStatement.setInt(1,fid);
        addBookingStatement.executeUpdate();
        // update reservation

        getReservationIdStatement.setInt(1, 0);
        ResultSet resultReservationId = getReservationIdStatement.executeQuery();
        resultReservationId.next();
        int reservationId = resultReservationId.getInt("id");
        // int reservationId;
        // if (resultReservationId.next()) {
        //   reservationId = resultReservationId.getInt("id");
        // }

        //System.out.println(resultReservationId.getInt("reservid"));

        resultReservationId.close();
        reserveStatement.clearParameters();
    		reserveStatement.setInt(1, reservationId);
    		reserveStatement.setString(2, username);
    		reserveStatement.setInt(3, date);
    		reserveStatement.setInt(4, fid);
        try {
    		reserveStatement.executeUpdate();
        returnStatement = "Booked flight(s), reservation ID: "+ reservationId + "\n";
        incrementIdStatement.executeUpdate();
    		} catch (SQLException e) {
          System.out.println("before");
    			if (isConstraintViolation(e)) {
            System.out.println("after");
    				returnStatement = "You cannot book two flights in the same day\n";
    			} else {
    				returnStatement = "Booking failed\n";
    			}
    			rollbackTransaction();
          return(returnStatement);
    		}
        //
        // try
        // {
        //   reserve_spot(username, fid, date, returnStatement);
        // } catch(SQLException e) { e.printStackTrace(); }

      } else {
        // passenger count
        //System.out.println("passenge time");
        int currentPassengerCount = resultPassengerCount.getInt("passengers_count");
        // capacity
        int currentCapacity = checkFlightCapacity(fid);
        // checkFlightCapacityStatement.clearParameters();
        // checkFlightCapacityStatement.setInt(1, fid);
        // ResultSet resultCurrentCapacity = checkFlightCapacityStatement.executeQuery();
        // //System.out.println("capacity time");
        // int currentCapacity = resultCurrentCapacity.getInt("capacity");
        if(currentPassengerCount < currentCapacity) {
          // update Booking
          updateBookingStatement.clearParameters();
          updateBookingStatement.setInt(1, currentPassengerCount + 1);
          updateBookingStatement.setInt(2, fid);
          updateBookingStatement.executeUpdate();


          getReservationIdStatement.setInt(1, 0);
          ResultSet resultReservationId = getReservationIdStatement.executeQuery();
          resultReservationId.next();
          int reservationId = resultReservationId.getInt("id");

          //resultReservationId.close();
          reserveStatement.clearParameters();
      		reserveStatement.setInt(1, reservationId);
      		reserveStatement.setString(2, username);
      		reserveStatement.setInt(3, date);
      		reserveStatement.setInt(4, fid);
          try {
      		reserveStatement.executeUpdate();
          returnStatement = "Booked flight(s), reservation ID: "+ reservationId + "\n";
          incrementIdStatement.executeUpdate();
      		} catch (SQLException e) {
      			if (isConstraintViolation(e)) {
      				returnStatement = "You cannot book two flights in the same day\n";
      			} else {
      				returnStatement = "Booking failed\n";
      			}
            rollbackTransaction();
            return(returnStatement);
      		}

          // update reservation
          // try
          // {
          //   reserve_spot(username, fid, date, returnStatement);
          // } catch(SQLException e) { e.printStackTrace(); }
        }
      }
    }
    commitTransaction();
  } catch (SQLException e) { e.printStackTrace(); }
  return(returnStatement);
  }


	private static boolean isConstraintViolation(SQLException e) {
	    return e.getSQLState().startsWith("23");
	}

  // check
  // private String reservationDayChecker(int day) {
  //   try
  //   {
  //     // get distinct day of month from reservation with given username.
  //     checkDay.setString(1, username);
  //     ResultSet result_day = checkDay.executeQuery();
  //
  //     while(result_day.next()) {
  //       int dday = result_day.getInt("day_of_month");
  //       if (dday == day) {
  //         return("You cannot book two flights in the same day\n");
  //       }
  //     }
  //   } catch (SQLException e) { e.printStackTrace(); }
  //   return ("Booking failed\n");
  // }

  /**
   * Implements the reservations function.
   *
   * @return If no user has logged in, then return "Cannot view reservations, not logged in\n"
   * If the user has no reservations, then return "No reservations found\n"
   * For all other errors, return "Failed to retrieve reservations\n"
   *
   * Otherwise return the reservations in the following format:
   *
   * Reservation [reservation ID] paid: [true or false]:\n"
   * [flight 1 under the reservation]
   * [flight 2 under the reservation]
   * Reservation [reservation ID] paid: [true or false]:\n"
   * [flight 1 under the reservation]
   * [flight 2 under the reservation]
   * ...
   *
   * Each flight should be printed using the same format as in the {@code Flight} class.
   *
   * @see Flight#toString()
   */
  public String transaction_reservations()
  {
    StringBuffer sb = new StringBuffer();
    if(username == null) {
      return("Cannot view reservations, not logged in\n");
    }
    try
    {
    beginTransaction();
    //Boolean existReservation = false;
    checkReservationStatement.clearParameters();
    checkReservationStatement.setString(1, username);
    ResultSet resultReservations = checkReservationStatement.executeQuery();
    if(resultReservations.isBeforeFirst()) {
      while(resultReservations.next()) {

        int rid = resultReservations.getInt("reservation_id");
        //int paid = resultReservations.getInt("paid");
        boolean paid = false;
        if(resultReservations.getInt("paid") == 1) {
          paid = true;
        }
        // if there exist two flight with same reservation number
        checkTwoHopReservation.setInt(1, rid);
        ResultSet resultCheckTwoHopReservation = checkTwoHopReservation.executeQuery();
        resultCheckTwoHopReservation.next();
        int count = resultCheckTwoHopReservation.getInt("count");
        int fid = resultReservations.getInt("fid");
        flightStatement.clearParameters();
        flightStatement.setInt(1, fid);
        ResultSet resultFlightStatement = flightStatement.executeQuery();
        resultFlightStatement.next();

        int result_dayOfMonth = resultFlightStatement.getInt("day_of_month");
        String result_carrierId = resultFlightStatement.getString("carrier_id");
        String result_flightNum = resultFlightStatement.getString("flight_num");
        String result_originCity = resultFlightStatement.getString("origin_city");
        String result_destCity = resultFlightStatement.getString("dest_city");
        int result_time = resultFlightStatement.getInt("actual_time");
        int result_capacity = resultFlightStatement.getInt("capacity");
        int result_price = resultFlightStatement.getInt("price");
        String reservation = "Reservation "+ rid + " paid: "+ paid + ":\n";
        String flight = "ID: " + fid + " Day: " + result_dayOfMonth + " Carrier: " + result_carrierId + " Number: " + result_flightNum + " Origin: " + result_originCity + " Destination: " + result_destCity + " Duration: " + result_time + " Capacity: " + result_capacity + " Price: " + result_price + "\n";
        sb.append(reservation);
        sb.append(flight);
        if(count == 2) {
            resultReservations.next();
            int fid1 = resultReservations.getInt("fid");
            flightStatement.clearParameters();
            flightStatement.setInt(1, fid1);
            ResultSet resultFlightStatement1 = flightStatement.executeQuery();
            resultFlightStatement1.next();

            int result_dayOfMonth1 = resultFlightStatement1.getInt("day_of_month");
            String result_carrierId1 = resultFlightStatement.getString("carrier_id");
            String result_flightNum1 = resultFlightStatement.getString("flight_num");
            String result_originCity1 = resultFlightStatement.getString("origin_city");
            String result_destCity1 = resultFlightStatement.getString("dest_city");
            int result_time1 = resultFlightStatement.getInt("actual_time");
            int result_capacity1 = resultFlightStatement.getInt("capacity");
            int result_price1 = resultFlightStatement.getInt("price");
            String flight1 = "ID: " + fid1 + " Day: " + result_dayOfMonth1 + " Carrier: " + result_carrierId1 + " Number: " + result_flightNum1 + " Origin: " + result_originCity1 + " Destination: " + result_destCity1 + " Duration: " + result_time1 + " Capacity: " + result_capacity1 + " Price: " + result_price1 + "\n";
            sb.append(flight1);
        }
      }
      commitTransaction();
      return(sb.toString());
    }
    rollbackTransaction();
  }
  catch (SQLException e)
  {
    e.printStackTrace();
  }

    return("No reservations found\n");
  }

  /**
   * Implements the cancel operation.
   *
   * @param reservationId the reservation ID to cancel
   *
   * @return If no user has logged in, then return "Cannot cancel reservations, not logged in\n"
   * For all other errors, return "Failed to cancel reservation [reservationId]"
   *
   * If successful, return "Canceled reservation [reservationId]"
   *
   * Even though a reservation has been canceled, its ID should not be reused by the system.
   */
  public String transaction_cancel(int reservationId)
  {
    // only implement this if you are interested in earning extra credit for the HW!
    return "Failed to cancel reservation " + reservationId;
  }

  /**
   * Implements the pay function.
   *
   * @param reservationId the reservation to pay for.
   *
   * @return If no user has logged in, then return "Cannot pay, not logged in\n"
   * If the reservation is not found / not under the logged in user's name, then return
   * "Cannot find unpaid reservation [reservationId] under user: [username]\n"
   * If the user does not have enough money in their account, then return
   * "User has only [balance] in account but itinerary costs [cost]\n"
   * For all other errors, return "Failed to pay for reservation [reservationId]\n"
   *
   * If successful, return "Paid reservation: [reservationId] remaining balance: [balance]\n"
   * where [balance] is the remaining balance in the user's account.
   */
  public String transaction_pay (int reservationId)
  {
    if(username == null) {
      return("Cannot pay, not logged in\n");
    }
    // If the reservation is not found / not under the logged in user's name
    try
    {
      beginTransaction();
      checkReservationStatement.clearParameters();
      checkReservationStatement.setString(1, username);
      ResultSet resultReservations = checkReservationStatement.executeQuery();
      if(!resultReservations.next()) {
        return("Cannot find unpaid reservation " + reservationId +" under user: "+ username +"\n");
      }
      // get user's account
      checkBalanceStatement.setString(1, username);
      ResultSet resultBalance = checkBalanceStatement.executeQuery();
      resultBalance.next();
      int balance = resultBalance.getInt("balance");
      // check Price
      checkTwoHopReservation.setInt(1, reservationId);
      ResultSet resultCheckTwoHopReservation = checkTwoHopReservation.executeQuery();
      int total_price = 0;
      while(resultCheckTwoHopReservation.next()) {
        int fid = resultCheckTwoHopReservation.getInt("fid");
        flightStatement.setInt(1, fid);
        ResultSet flightPrice = flightStatement.executeQuery();
        flightPrice.next();
        int price = flightPrice.getInt("price");
        total_price += price;
      }

      if(balance >= total_price) {
        // update paid in reservation table
        updatePaid.setInt(1, reservationId);
        updatePaid.executeUpdate();
        // update balance of users
        int afterBalance = balance - total_price;
        updateBalancestatement.setInt(1, afterBalance);
        updateBalancestatement.setString(2,username);
        updateBalancestatement.executeUpdate();
        commitTransaction();
        return("Paid reservation:"+ reservationId + "remaining balance: "+ afterBalance + "\n");
      }
      rollbackTransaction();
    }
    catch(SQLException e)
    {

      return ("Failed to pay for reservation " + reservationId + "\n");

    }
    return ("Failed to pay for reservation " + reservationId + "\n");
  }

  /* some utility functions below */

  public void beginTransaction() throws SQLException
  {
    conn.setAutoCommit(false);
    beginTransactionStatement.executeUpdate();
  }

  public void commitTransaction() throws SQLException
  {
    commitTransactionStatement.executeUpdate();
    conn.setAutoCommit(true);
  }

  public void rollbackTransaction() throws SQLException
  {
    rollbackTransactionStatement.executeUpdate();
    conn.setAutoCommit(true);
  }

  /**
   * Shows an example of using PreparedStatements after setting arguments. You don't need to
   * use this method if you don't want to.
   */
  private int checkFlightCapacity(int fid) throws SQLException
  {
    checkFlightCapacityStatement.clearParameters();
    checkFlightCapacityStatement.setInt(1, fid);
    ResultSet results = checkFlightCapacityStatement.executeQuery();
    results.next();
    int capacity = results.getInt("capacity");
    results.close();

    return capacity;
  }
}
