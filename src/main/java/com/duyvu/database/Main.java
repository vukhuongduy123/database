package com.duyvu.database;

import com.duyvu.database.schema.*;
import com.duyvu.database.server.Server;
import java.io.IOException;
import java.util.*;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class Main {
  static void main() throws IOException {
    Server server = new Server(8080, 1);
    server.start();
  }
}
