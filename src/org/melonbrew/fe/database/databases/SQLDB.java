package org.melonbrew.fe.database.databases;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.melonbrew.fe.Fe;
import org.melonbrew.fe.database.Account;
import org.melonbrew.fe.database.Database;

public abstract class SQLDB extends Database {

    private final Fe plugin;

    private final boolean supportsModification;

    private Connection connection;

    private String accountsName;

    private String versionName;

    private String accountsColumnUser;

    private String accountsColumnMoney;

    private String accountsColumnUUID;

    public SQLDB(Fe plugin, boolean supportsModification) {
        super(plugin);

        this.plugin = plugin;

        this.supportsModification = supportsModification;

        accountsName = "fe_accounts";

        versionName = "fe_version";

        accountsColumnUser = "name";

        accountsColumnMoney = "money";

        accountsColumnUUID = "uuid";

        plugin.getServer().getScheduler().runTaskTimerAsynchronously(plugin, new Runnable() {
            @Override
            public void run() {
                try {
                    if (connection != null && !connection.isClosed()) {
                        connection.createStatement().execute("/* ping */ SELECT 1");
                    }
                } catch (SQLException e) {
                    connection = getNewConnection();
                }
            }
        }, 60 * 20, 60 * 20);
    }

    public void setAccountTable(String accountsName) {
        this.accountsName = accountsName;
    }

    public void setVersionTable(String versionName) {
        this.versionName = versionName;
    }

    public void setAccountsColumnUser(String accountsColumnUser) {
        this.accountsColumnUser = accountsColumnUser;
    }

    public void setAccountsColumnMoney(String accountsColumnMoney) {
        this.accountsColumnMoney = accountsColumnMoney;
    }

    public void setAccountsColumnUUID(String accountsColumnUUID) {
        this.accountsColumnUUID = accountsColumnUUID;
    }

    @Override
    public boolean init() {
        super.init();
        checkConnection();
        try {
            this.setupTables();
        } catch (SQLException ex) {
            Logger.getLogger(SQLDB.class.getName()).log(Level.SEVERE, null, ex);
        }
        return checkConnection();

    }

    public boolean checkConnection() {
        try {
            if (connection == null || connection.isClosed()) {
                connection = getNewConnection();
                if (connection == null || connection.isClosed()) {
                    return false;
                } else {
                    setVersion(1);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();

            return false;
        }

        return true;
    }

    private void setupTables() throws SQLException {
        query("CREATE TABLE IF NOT EXISTS " + accountsName + " (" + accountsColumnUser + " varchar(64) NOT NULL, " + accountsColumnUUID + " varchar(36), " + accountsColumnMoney + " double NOT NULL)");
        query("CREATE TABLE IF NOT EXISTS " + versionName + " (version int NOT NULL)");
    }

    protected abstract Connection getNewConnection();

    public boolean query(String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            return statement.execute(sql);
        }
    }

    @Override
    public void close() {
        super.close();

        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int getVersion() {
        checkConnection();

        int version = 0;

        try {
            try (ResultSet set = connection.prepareStatement("SELECT * from " + versionName).executeQuery()) {
                if (set.next()) {
                    version = set.getInt("version");
                }
            }

            return version;
        } catch (Exception e) {
            e.printStackTrace();

            return version;
        }
    }

    @Override
    public void setVersion(int version) {
        checkConnection();

        try {
            connection.prepareStatement("DELETE FROM " + versionName).executeUpdate();

            connection.prepareStatement("INSERT INTO " + versionName + " (version) VALUES (" + version + ")").executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<Account> loadTopAccounts(int size) {
        checkConnection();
        String sql = "SELECT * FROM " + accountsName + " ORDER BY money DESC limit " + size;
        List<Account> topAccounts = new ArrayList<>();
        try {
            try (Statement st = connection.createStatement()) {
                ResultSet set = st.executeQuery(sql);
                while (set.next()) {
                    Account account = new Account(plugin, set.getString(accountsColumnUser), set.getString(accountsColumnUUID), this);
                    account.setMoney(set.getDouble(accountsColumnMoney));
                    topAccounts.add(account);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return topAccounts;
    }

    @Override
    public List<Account> getAccounts() {
        checkConnection();
        List<Account> accounts = new ArrayList<>();
        try {
            try (Statement st = connection.createStatement()) {
                ResultSet set = st.executeQuery("SELECT * from " + accountsName);
                while (set.next()) {
                    Account account = new Account(plugin, set.getString(accountsColumnUser), set.getString(accountsColumnUUID), this);
                    account.setMoney(set.getDouble(accountsColumnMoney));
                    accounts.add(account);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return accounts;
    }

    @Override
    public HashMap<String, String> loadAccountData(String name, String uuid) {
        checkConnection();

        try {
            HashMap<String, String> data;
            try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + accountsName + " WHERE UPPER(" + (uuid != null ? accountsColumnUUID : accountsColumnUser) + ") LIKE UPPER(?)")) {
                statement.setString(1, uuid != null ? uuid : name);
                ResultSet set = statement.executeQuery();
                data = new HashMap<>();
                while (set.next()) {
                    data.put("money", set.getString(accountsColumnMoney));
                    data.put("name", set.getString(accountsColumnUser));
                }
            }
            return data;
        } catch (SQLException e) {
            e.printStackTrace();

            return null;
        }
    }

    @Override
    public void removeAccount(String name, String uuid) {
        super.removeAccount(name, uuid);
        checkConnection();
        try {
            try (PreparedStatement statement = connection.prepareStatement("DELETE FROM " + accountsName + " WHERE UPPER(" + (uuid != null ? accountsColumnUUID : accountsColumnUser) + ") LIKE UPPER(?)")) {
                statement.setString(1, uuid != null ? uuid : name);
                statement.execute();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void saveAccount(String name, String uuid, double money) {
        checkConnection();
        try {
            String sql = "UPDATE " + accountsName + " SET " + accountsColumnMoney + "=?, " + accountsColumnUser + "=? WHERE UPPER(";
            if (uuid != null) {
                sql += accountsColumnUUID;
            } else {
                sql += accountsColumnUser;
            }
            try (PreparedStatement statement = connection.prepareStatement(sql + ") LIKE UPPER(?)")) {
                statement.setDouble(1, money);
                statement.setString(2, name);
                if (uuid != null) {
                    statement.setString(3, uuid);
                } else {
                    statement.setString(3, name);
                }
                if (statement.executeUpdate() == 0) {
                    try (PreparedStatement insert = connection.prepareStatement("INSERT INTO " + accountsName + " (" + accountsColumnUser + ", " + accountsColumnUUID + ", " + accountsColumnMoney + ") VALUES (?, ?, ?)")) {
                        insert.setString(1, name);
                        insert.setString(2, uuid);
                        insert.setDouble(3, money);
                        insert.execute();
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void clean() {
        checkConnection();
        try {
            boolean executeQuery;
            StringBuilder builder;
            try (Statement st = connection.createStatement()) {
                ResultSet set = st.executeQuery("SELECT * from " + accountsName + " WHERE " + accountsColumnMoney + "=" + plugin.getAPI().getDefaultHoldings());
                executeQuery = false;
                builder = new StringBuilder("DELETE FROM " + accountsName + " WHERE " + accountsColumnUser + " IN (");
                while (set.next()) {
                    String name = set.getString(accountsColumnUser);
                    if (plugin.getServer().getPlayerExact(name) != null) {
                        continue;
                    }
                    executeQuery = true;
                    builder.append("'").append(name).append("', ");
                }
            }
            builder.delete(builder.length() - 2, builder.length()).append(")");
            if (executeQuery) {
                query(builder.toString());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void removeAllAccounts() {
        super.removeAllAccounts();
        checkConnection();
        try {
            try (Statement st = connection.createStatement()) {
                st.executeUpdate("DELETE FROM " + accountsName);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
