package name.nkonev.r2dbc.migrate.core;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.List;

public abstract class LogCaptureableTests {

  protected ListAppender<ILoggingEvent> startAppender() {
    getStatementsLogger().setLevel(Level.DEBUG); // TODO here I override maven logger
    ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
    listAppender.start();
    // add the appender to the logger
    // addAppender is outdated now
    getStatementsLogger().addAppender(listAppender);
    return listAppender;
  }

  protected List<ILoggingEvent> stopAppenderAndGetLogsList(ListAppender<ILoggingEvent> listAppender) {
    List<ILoggingEvent> logsList = listAppender.list;
    listAppender.stop();
    getStatementsLogger().detachAppender(listAppender);
    getStatementsLogger().setLevel(getStatementsPreviousLevel());
    return logsList;
  }

  protected abstract Level getStatementsPreviousLevel();

  protected abstract Logger getStatementsLogger();

}
