package org.reveno.atp.acceptance.tests;

import org.junit.Assert;
import org.junit.Test;
import org.reveno.atp.acceptance.api.commands.CreateNewAccountCommand;
import org.reveno.atp.acceptance.api.commands.NewOrderCommand;
import org.reveno.atp.acceptance.api.events.AccountCreatedEvent;
import org.reveno.atp.acceptance.api.events.OrderCreatedEvent;
import org.reveno.atp.acceptance.api.transactions.AcceptOrder;
import org.reveno.atp.acceptance.api.transactions.CreateAccount;
import org.reveno.atp.acceptance.api.transactions.Credit;
import org.reveno.atp.acceptance.api.transactions.Debit;
import org.reveno.atp.acceptance.handlers.RollbackTransactions;
import org.reveno.atp.acceptance.handlers.Transactions;
import org.reveno.atp.acceptance.model.Account;
import org.reveno.atp.acceptance.model.Order.OrderType;
import org.reveno.atp.acceptance.views.AccountView;
import org.reveno.atp.acceptance.views.OrderView;
import org.reveno.atp.api.Configuration.ModelType;
import org.reveno.atp.api.Configuration.MutableModelFailover;
import org.reveno.atp.api.Reveno;
import org.reveno.atp.api.commands.EmptyResult;
import org.reveno.atp.api.domain.Repository;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.IntStream;

public class Tests extends RevenoBaseTest {

    /**
     * 测试基本 重播使用
     * @throws Exception
     */
    @Test
    public void testBasic() throws Exception {
        Reveno reveno = createEngine();
        reveno.startup();

        Waiter accountCreatedEvent = listenFor(reveno, AccountCreatedEvent.class);
        Waiter orderCreatedEvent = listenFor(reveno, OrderCreatedEvent.class);
        long accountId = sendCommandSync(reveno, new CreateNewAccountCommand("USD", 1000_000L));
        AccountView accountView = reveno.query().find(AccountView.class, accountId);

        Assert.assertTrue(accountCreatedEvent.isArrived());
        Assert.assertEquals(accountId, accountView.accountId);
        Assert.assertEquals("USD", accountView.currency);
        Assert.assertEquals(1000_000L, accountView.balance);
        Assert.assertEquals(0, accountView.orders().size());

        long orderId = sendCommandSync(reveno, new NewOrderCommand(accountId, null, "EUR/USD", 134000, 1000, OrderType.MARKET));
        OrderView orderView = reveno.query().find(OrderView.class, orderId);
        accountView = reveno.query().find(AccountView.class, accountId);

        Assert.assertTrue(orderCreatedEvent.isArrived());
        Assert.assertEquals(orderId, orderView.id);
        Assert.assertEquals(1, accountView.orders().size());

        reveno.shutdown();
    }

    /**
     * 测试异步处理程序
     * @throws Exception
     */
    @Test
    public void testAsyncHandlers() throws Exception {
        Reveno reveno = createEngine();
        reveno.startup();

        Waiter accountCreatedEvent = listenAsyncFor(reveno, AccountCreatedEvent.class, 1_000);
        sendCommandsBatch(reveno, new CreateNewAccountCommand("USD", 1000_000L), 1_000);
        Assert.assertTrue(accountCreatedEvent.isArrived());

        reveno.shutdown();
    }

    /**
     * 测试异常事件处理程序
     * @throws Exception
     */
    @Test
    public void testExceptionalEventHandler() throws Exception {
        Reveno reveno = createEngine();
        reveno.startup();

        Waiter w = listenFor(reveno, AccountCreatedEvent.class, 1_000, (c) -> {
            if (c == 500 || c == 600 || c == 601) {
                throw new RuntimeException();
            }
        });
        sendCommandsBatch(reveno, new CreateNewAccountCommand("USD", 1000_000L), 1_000);
        // it's just fine since on exception we still processing
        // but such events won't be committed
        Assert.assertTrue(w.isArrived());

        reveno.shutdown();

        reveno = createEngine();
        w = listenFor(reveno, AccountCreatedEvent.class, 4);
        reveno.startup();

        // after restart we expect that there will be 3 replayed
        // events - the count of exceptions
        Assert.assertFalse(w.isArrived(1));
        Assert.assertEquals(1, w.getCount());

        reveno.shutdown();
    }

    /**
     * 测试异常异步事件处理程序
     * @throws Exception
     */
    @Test
    public void testExceptionalAsyncEventHandler() throws Exception {
        TestRevenoEngine reveno = createEngine();
        reveno.events().asyncEventExecutors(10);
        reveno.startup();

        Waiter w = listenAsyncFor(reveno, AccountCreatedEvent.class, 1_000, (c) -> {
            if (c == 500 || c == 600 || c == 601) {
                throw new RuntimeException();
            }
        });
        sendCommandsBatch(reveno, new CreateNewAccountCommand("USD", 1000_000L), 1_000);
        Assert.assertTrue(w.isArrived(5));

        reveno.syncAll();
        reveno.shutdown();

        reveno = createEngine();
        w = listenFor(reveno, AccountCreatedEvent.class, 4);
        reveno.startup();

        Assert.assertFalse(w.isArrived(1));
        Assert.assertEquals(1, w.getCount());

        reveno.shutdown();
    }

    /**
     * 测试批次
     * @throws Exception
     */
    @Test
    public void testBatch() throws Exception {
        Reveno reveno = createEngine();
        Waiter accountsWaiter = listenFor(reveno, AccountCreatedEvent.class, 10_000);
        Waiter ordersWaiter = listenFor(reveno, OrderCreatedEvent.class, 10_000);
        reveno.startup();

        generateAndSendCommands(reveno, 10_000);

        Assert.assertEquals(10_000, reveno.query().select(AccountView.class).size());
        Assert.assertEquals(10_000, reveno.query().select(OrderView.class).size());

        Assert.assertTrue(accountsWaiter.isArrived());
        Assert.assertTrue(ordersWaiter.isArrived());

        reveno.shutdown();
    }

    /**
     * 测试重播
     * @throws Exception
     */
    @Test
    public void testReplay() throws Exception {
        testBasic();

        Reveno reveno = createEngine();
        Waiter accountCreatedEvent = listenFor(reveno, AccountCreatedEvent.class);
        Waiter orderCreatedEvent = listenFor(reveno, OrderCreatedEvent.class);
        reveno.startup();

        Assert.assertFalse(accountCreatedEvent.isArrived(1));
        Assert.assertFalse(orderCreatedEvent.isArrived(1));

        Assert.assertEquals(1, reveno.query().select(AccountView.class).size());
        Assert.assertEquals(1, reveno.query().select(OrderView.class).size());

        reveno.shutdown();
    }

    /**
     * 测试批量重播
     * @throws Exception
     */
    @Test
    public void testBatchReplay() throws Exception {
        testBatch();

        Reveno reveno = createEngine();
        Waiter accountsWaiter = listenFor(reveno, AccountCreatedEvent.class, 1);
        Waiter ordersWaiter = listenFor(reveno, OrderCreatedEvent.class, 1);
        reveno.startup();

        Assert.assertEquals(10_000, reveno.query().select(AccountView.class).size());
        Assert.assertEquals(10_000, reveno.query().select(OrderView.class).size());

        Assert.assertFalse(accountsWaiter.isArrived(1));
        Assert.assertFalse(ordersWaiter.isArrived(1));

        long accountId = sendCommandSync(reveno, new CreateNewAccountCommand("USD", 1000_000L));
        Assert.assertEquals(10_001, accountId);
        long orderId = sendCommandSync(reveno, new NewOrderCommand(accountId, null, "EUR/USD", 134000, 1000, OrderType.MARKET));
        Assert.assertEquals(10_001, orderId);

        reveno.shutdown();
    }

    /**
     * 测试平行滚动
     * @throws Exception
     */
    @Test
    public void testParallelRolling() throws Exception {
        final boolean[] stop = {false};
        AtomicLong counter = new AtomicLong(0);
        ExecutorService transactionExecutor = Executors.newFixedThreadPool(10);
        TestRevenoEngine reveno = createEngine();
        reveno.startup();
        IntStream.range(0, 10).forEach(i -> transactionExecutor.submit(() -> {
            while (!stop[0]) {
                counter.incrementAndGet();
                try {
                    sendCommandSync(reveno, new CreateNewAccountCommand("USD", 1000_000L));
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        }));

        IntStream.range(0, 20).forEach(i -> {
            Waiter w = new Waiter(1);
            reveno.roll(w::countDown);
            w.awaitSilent();
            sleep(200);
        });
        stop[0] = true;
        sleep(10);
        transactionExecutor.shutdown();

        reveno.shutdown();

        Reveno revenoRestarted = createEngine();
        Waiter accountCreatedEvent = listenFor(reveno, AccountCreatedEvent.class);
        revenoRestarted.startup();

        Assert.assertFalse(accountCreatedEvent.isArrived(1));
        Assert.assertEquals(counter.get(), reveno.query().select(AccountView.class).size());

        revenoRestarted.shutdown();
    }

    /**
     * 使用补偿动作测试交易
     * @throws Exception
     */
    @Test
    public void testTransactionWithCompensatingActions() throws Exception {
        if (modelType != ModelType.MUTABLE) {
            return;
        }

        class TestTx {
        }
        class TestCmd {
        }

        Repository[] repo = new Repository[1];
        Consumer<TestRevenoEngine> consumer = r -> {
            r.config().mutableModel().mutableModelFailover(MutableModelFailover.COMPENSATING_ACTIONS);
            r.domain().transactionWithCompensatingAction(CreateAccount.class, Transactions::createAccount, RollbackTransactions::rollbackCreateAccount);
            r.domain().transactionWithCompensatingAction(AcceptOrder.class, Transactions::acceptOrder, RollbackTransactions::rollbackAcceptOrder);
            r.domain().transactionWithCompensatingAction(Credit.class, Transactions::credit, RollbackTransactions::rollbackCredit);
            r.domain().transactionWithCompensatingAction(Debit.class, Transactions::debit, RollbackTransactions::rollbackDebit);

            r.domain().command(TestCmd.class, (c, d) -> d.executeTxAction(new TestTx()));
            r.domain().transactionAction(TestTx.class, (a, b) -> {
                repo[0] = b.repo();
                throw new RuntimeException();
            });
        };
        Reveno reveno = createEngine(consumer);
        reveno.startup();

        long accountId = sendCommandSync(reveno, new CreateNewAccountCommand("USD", 1000));
        Future<EmptyResult> f = reveno.performCommands(Arrays.asList(new Credit(accountId, 15, 0), new Debit(accountId, 8),
                new NewOrderCommand(accountId, null, "EUR/USD", 134000, 1, OrderType.MARKET), new TestCmd()));

        Assert.assertFalse(f.get().isSuccess());
        Assert.assertEquals(RuntimeException.class, f.get().getException().getClass());
        Assert.assertEquals(1000, repo[0].get(Account.class, accountId).balance());
        Assert.assertEquals(1000, reveno.query().find(AccountView.class, accountId).balance);

        reveno.shutdown();

        reveno = createEngine(consumer);
        reveno.startup();

        Assert.assertEquals(1000, reveno.query().find(AccountView.class, accountId).balance);

        reveno.shutdown();
    }

}
