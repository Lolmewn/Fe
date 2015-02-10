package org.melonbrew.fe.database.databases;

import org.bukkit.configuration.ConfigurationSection;
import org.melonbrew.fe.Fe;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;

public class SQLiteDB extends SQLDB {
    private final Fe plugin;

    public SQLiteDB(Fe plugin) {
        super(plugin, false);

        this.plugin = plugin;
    }

    @Override
    public Connection getNewConnection() {
        try {
            Class.forName("org.sqlite.JDBC");

            return DriverManager.getConnection("jdbc:sqlite:" + new File(plugin.getDataFolder(), "database.db").getAbsolutePath());
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void getConfigDefaults(ConfigurationSection section) {

    }

    @Override
    public String getName() {
        return "SQLite";
    }
}
