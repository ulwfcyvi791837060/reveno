package org.reveno.atp.core.disruptor;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.reveno.atp.api.Configuration.CpuConsumption;
import org.reveno.atp.commons.NamedThreadFactory;
import org.reveno.atp.core.RevenoConfiguration;
import org.reveno.atp.core.api.EventsCommitInfo;
import org.reveno.atp.core.api.EventsCommitInfo.Builder;
import org.reveno.atp.core.api.Journaler;
import org.reveno.atp.core.api.serialization.EventsInfoSerializer;
import org.reveno.atp.core.data.DefaultJournaler;
import org.reveno.atp.core.engine.processor.PipeProcessor;
import org.reveno.atp.core.events.Event;
import org.reveno.atp.core.events.EventHandlersManager;
import org.reveno.atp.core.events.EventPublisher;
import org.reveno.atp.core.events.EventsContext;
import org.reveno.atp.core.impl.EventsCommitInfoImpl;
import org.reveno.atp.core.serialization.SimpleEventsSerializer;
import org.reveno.atp.core.storage.FileSystemStorage;
import org.reveno.atp.test.utils.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class DisruptorEventsBusTest {
	
	private File tempDir;
	private FileSystemStorage storage;
	
	@Before 
	public void setUp() {
		tempDir = Files.createTempDir();
		storage = new FileSystemStorage(tempDir, new RevenoConfiguration.RevenoJournalingConfiguration());
	}
	
	@After 
	public void tearDown() throws IOException {
		FileUtils.delete(tempDir);
	}

	/**
	 * "C:\Program Files\Java\jdk1.8.0_181\bin\java.exe" -ea -Didea.test.cyclic.buffer.size=1048576 "-javaagent:C:\Program Files\JetBrains\IntelliJ IDEA 2018.3.2\lib\idea_rt.jar=52760:C:\Program Files\JetBrains\IntelliJ IDEA 2018.3.2\bin" -Dfile.encoding=UTF-8 -classpath "C:\Program Files\JetBrains\IntelliJ IDEA 2018.3.2\lib\idea_rt.jar;C:\Program Files\JetBrains\IntelliJ IDEA 2018.3.2\plugins\junit\lib\junit-rt.jar;C:\Program Files\JetBrains\IntelliJ IDEA 2018.3.2\plugins\junit\lib\junit5-rt.jar;C:\Program Files\Java\jdk1.8.0_181\jre\lib\charsets.jar;C:\Program Files\Java\jdk1.8.0_181\jre\lib\deploy.jar;C:\Program Files\Java\jdk1.8.0_181\jre\lib\ext\access-bridge-64.jar;C:\Program Files\Java\jdk1.8.0_181\jre\lib\ext\cldrdata.jar;C:\Program Files\Java\jdk1.8.0_181\jre\lib\ext\dnsns.jar;C:\Program Files\Java\jdk1.8.0_181\jre\lib\ext\jaccess.jar;C:\Program Files\Java\jdk1.8.0_181\jre\lib\ext\jfxrt.jar;C:\Program Files\Java\jdk1.8.0_181\jre\lib\ext\localedata.jar;C:\Program Files\Java\jdk1.8.0_181\jre\lib\ext\nashorn.jar;C:\Program Files\Java\jdk1.8.0_181\jre\lib\ext\sunec.jar;C:\Program Files\Java\jdk1.8.0_181\jre\lib\ext\sunjce_provider.jar;C:\Program Files\Java\jdk1.8.0_181\jre\lib\ext\sunmscapi.jar;C:\Program Files\Java\jdk1.8.0_181\jre\lib\ext\sunpkcs11.jar;C:\Program Files\Java\jdk1.8.0_181\jre\lib\ext\zipfs.jar;C:\Program Files\Java\jdk1.8.0_181\jre\lib\javaws.jar;C:\Program Files\Java\jdk1.8.0_181\jre\lib\jce.jar;C:\Program Files\Java\jdk1.8.0_181\jre\lib\jfr.jar;C:\Program Files\Java\jdk1.8.0_181\jre\lib\jfxswt.jar;C:\Program Files\Java\jdk1.8.0_181\jre\lib\jsse.jar;C:\Program Files\Java\jdk1.8.0_181\jre\lib\management-agent.jar;C:\Program Files\Java\jdk1.8.0_181\jre\lib\plugin.jar;C:\Program Files\Java\jdk1.8.0_181\jre\lib\resources.jar;C:\Program Files\Java\jdk1.8.0_181\jre\lib\rt.jar;D:\workspace_api\reveno\reveno-core\out\test\classes;D:\workspace_api\reveno\reveno-core\out\test\resources;D:\workspace_api\reveno\reveno-core\out\production\classes;D:\workspace_api\reveno\reveno-core\out\production\resources;D:\workspace_api\reveno\reveno-test-model\out\production\classes;C:\Users\Administrator\.gradle\caches\modules-2\files-2.1\org.slf4j\slf4j-log4j12\1.7.26\12f5c685b71c3027fd28bcf90528ec4ec74bf818\slf4j-log4j12-1.7.26.jar;C:\Users\Administrator\.gradle\caches\modules-2\files-2.1\org.slf4j\slf4j-api\1.7.26\77100a62c2e6f04b53977b9f541044d7d722693d\slf4j-api-1.7.26.jar;C:\Users\Administrator\.gradle\caches\modules-2\files-2.1\it.unimi.dsi\fastutil\8.2.2\975aab42e32a96ecb9696971a87c87a049055452\fastutil-8.2.2.jar;D:\maven\LocalUl\com\lmax\disruptor\3.4.2\disruptor-3.4.2.jar;C:\Users\Administrator\.gradle\caches\modules-2\files-2.1\io.protostuff\protostuff-uberjar\1.5.9\861b9b9a7c33abfedfc1741f4b78e99b180bbe82\protostuff-uberjar-1.5.9.jar;C:\Users\Administrator\.gradle\caches\modules-2\files-2.1\io.netty\netty-buffer\4.1.34.Final\8afc2027eadc7848127553ebb490ea0e2b199d4e\netty-buffer-4.1.34.Final.jar;C:\Users\Administrator\.gradle\caches\modules-2\files-2.1\net.bytebuddy\byte-buddy\1.9.12\39050dbbd36862ea87eb9a64158854b04619ccd6\byte-buddy-1.9.12.jar;C:\Users\Administrator\.gradle\caches\modules-2\files-2.1\com.github.stephenc.high-scale-lib\high-scale-lib\1.1.4\93865cc75c598f67a7a98e259b2ecfceec9a132\high-scale-lib-1.1.4.jar;C:\Users\Administrator\.gradle\caches\modules-2\files-2.1\com.google.guava\guava\27.1-jre\e47b59c893079b87743cdcfb6f17ca95c08c592c\guava-27.1-jre.jar;D:\maven\LocalUl\junit\junit\4.12\junit-4.12.jar;C:\Users\Administrator\.gradle\caches\modules-2\files-2.1\org.easymock\easymock\4.0.2\f74aebbe02f5051bea31c0dbc5df5202a59e0b78\easymock-4.0.2.jar;D:\maven\LocalUl\log4j\log4j\1.2.17\log4j-1.2.17.jar;C:\Users\Administrator\.gradle\caches\modules-2\files-2.1\io.protostuff\protostuff-core\1.5.9\baf9e29cc554b1ed905bfb9428b7566efb2cc9fd\protostuff-core-1.5.9.jar;C:\Users\Administrator\.gradle\caches\modules-2\files-2.1\io.protostuff\protostuff-runtime\1.5.9\6809a6d5fa97491627d701d97c0319f1acca418b\protostuff-runtime-1.5.9.jar;C:\Users\Administrator\.gradle\caches\modules-2\files-2.1\io.protostuff\protostuff-collectionschema\1.5.9\64de0c026d4e7da2110ac6af4ecfd5cf4c4d2e92\protostuff-collectionschema-1.5.9.jar;C:\Users\Administrator\.gradle\caches\modules-2\files-2.1\io.protostuff\protostuff-api\1.5.9\91f8cefbb3567b6d07c5478284c64d392aa477da\protostuff-api-1.5.9.jar;C:\Users\Administrator\.gradle\caches\modules-2\files-2.1\io.netty\netty-common\4.1.34.Final\2dffa21967d36cae446b6905bd5fb39750fcba43\netty-common-4.1.34.Final.jar;C:\Users\Administrator\.gradle\caches\modules-2\files-2.1\com.google.guava\failureaccess\1.0.1\1dcf1de382a0bf95a3d8b0849546c88bac1292c9\failureaccess-1.0.1.jar;C:\Users\Administrator\.gradle\caches\modules-2\files-2.1\com.google.guava\listenablefuture\9999.0-empty-to-avoid-conflict-with-guava\b421526c5f297295adef1c886e5246c39d4ac629\listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar;C:\Users\Administrator\.gradle\caches\modules-2\files-2.1\com.google.code.findbugs\jsr305\3.0.2\25ea2e8b0c338a877313bd4672d3fe056ea78f0d\jsr305-3.0.2.jar;C:\Users\Administrator\.gradle\caches\modules-2\files-2.1\org.checkerframework\checker-qual\2.5.2\cea74543d5904a30861a61b4643a5f2bb372efc4\checker-qual-2.5.2.jar;C:\Users\Administrator\.gradle\caches\modules-2\files-2.1\com.google.errorprone\error_prone_annotations\2.2.0\88e3c593e9b3586e1c6177f89267da6fc6986f0c\error_prone_annotations-2.2.0.jar;D:\maven\LocalUl\com\google\j2objc\j2objc-annotations\1.1\j2objc-annotations-1.1.jar;C:\Users\Administrator\.gradle\caches\modules-2\files-2.1\org.codehaus.mojo\animal-sniffer-annotations\1.17\f97ce6decaea32b36101e37979f8b647f00681fb\animal-sniffer-annotations-1.17.jar;D:\maven\LocalUl\org\hamcrest\hamcrest-core\1.3\hamcrest-core-1.3.jar;C:\Users\Administrator\.gradle\caches\modules-2\files-2.1\org.objenesis\objenesis\3.0.1\11cfac598df9dc48bb9ed9357ed04212694b7808\objenesis-3.0.1.jar" com.intellij.rt.execution.junit.JUnitStarter -ideVersion5 -junit4 org.reveno.atp.core.disruptor.DisruptorEventsBusTest,test
	 * 2019/10/26 15:09:58.279 |main| INFO [DefaultJournaler:40] Started writing to evn-2019_10_26-00000000000000000001-00000000000000000000
	 * 2019/10/26 15:09:58.339 |main| INFO [DisruptorPipeProcessor:56] Started.
	 * 2019/10/26 15:09:58.434 |main| INFO [DisruptorPipeProcessor:66] Stopped.
	 * 2019/10/26 15:09:58.435 |main| INFO [DefaultJournaler:49] Stopped writing to evn-2019_10_26-00000000000000000001-00000000000000000000
	 * 2019/10/26 15:09:58.435 |main| INFO [FileChannel:60] Closing channel C:\Users\ADMINI~1\AppData\Local\Temp\1572073798027-0\evn-2019_10_26-00000000000000000001-00000000000000000000
	 * 34
	 *
	 * Process finished with exit code 0
	 * @throws InterruptedException
	 */
	@Test
	public void test() throws InterruptedException {
		final MyEvent[] event = new MyEvent[1];
		CountDownLatch latch = new CountDownLatch(3);
		
		EventHandlersManager manager = new EventHandlersManager();
		manager.eventHandler(MyEvent.class, (e, md) -> { event[0] = e; latch.countDown(); });
		
		manager.eventHandler(MyNextEvent.class, (e, md) -> latch.countDown());
		
		String fileAddress = storage.nextStore().getEventsCommitsAddress();
		Journaler journaler = new DefaultJournaler();
		journaler.startWriting(storage.channel(fileAddress));
		
		EventsCommitInfo.Builder builder = new EventsCommitInfoImpl.PojoBuilder();
		EventsInfoSerializer serializer = new SimpleEventsSerializer();
		
		PipeProcessor<Event> pipe = new DisruptorEventPipeProcessor(CpuConsumption.HIGH, 1024, new NamedThreadFactory("bt"));

		//添加到管子里排队 排队处理完再处理日志
		EventPublisher eventsBus = new EventPublisher(pipe, new Context(journaler, builder, serializer, manager));
		eventsBus.getPipe().start();
		
		eventsBus.publishEvents(false, 5L, null, new Object[] { new MyEvent("Hello!"), new MyNextEvent() });
		eventsBus.publishEvents(false, 6L, null, new Object[] { new MyNextEvent() });
		
		latch.await(1000, TimeUnit.MILLISECONDS);
		Assert.assertNotNull(event[0]);
		Assert.assertEquals(event[0].message, "Hello!");
		
		eventsBus.getPipe().stop();
		journaler.stopWriting();
		
		System.out.println("File.length() ==> "+new File(tempDir, fileAddress).length());
	}
	
	public static class MyEvent {
		public String message;
		
		public MyEvent(String message) {
			this.message = message;
		}
	}
	
	public static class MyNextEvent {
		
	}
	
	public static class Context implements EventsContext {

		private Journaler journaler;
		@Override
		public Journaler eventsJournaler() {
			return journaler;
		}

		private Builder builder;
		@Override
		public Builder eventsCommitBuilder() {
			return builder;
		}

		private EventsInfoSerializer serializer;
		@Override
		public EventsInfoSerializer serializer() {
			return serializer;
		}

		private EventHandlersManager manager;
		@Override
		public EventHandlersManager manager() {
			return manager;
		}
		
		public Context(Journaler journaler, Builder builder, EventsInfoSerializer serializer, EventHandlersManager manager) {
			this.journaler = journaler;
			this.builder = builder;
			this.serializer = serializer;
			this.manager = manager;
		}
	}
	
}
