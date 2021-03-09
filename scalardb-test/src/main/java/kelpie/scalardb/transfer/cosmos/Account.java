package kelpie.scalardb.transfer.cosmos;

public class Account {
  private String id;
  private int accountId;
  private int type;
  private int balance;

  public Account() {}

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public int getAccountId() {
    return accountId;
  }

  public void setAccountId(int accountId) {
    this.accountId = accountId;
  }

  public int getType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
  }

  public int getBalance() {
    return balance;
  }

  public void setBalance(int balance) {
    this.balance = balance;
  }

  @Override
  public String toString() {
    return id + ":" + type + ":" + balance;
  }
}
