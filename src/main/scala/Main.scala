import akka.actor.ActorSystem
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.ActorMaterializer
import akka.stream.Supervision.resumingDecider
import akka.stream.scaladsl.Source
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.DefaultAwsRegionProviderChain
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import scala.collection.JavaConverters._
import scala.concurrent.{Future, blocking}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

object Main extends App {
  implicit val system = ActorSystem("s3storageclass")
  implicit val ec = system.dispatcher
  implicit val mat = ActorMaterializer()

  val awsCredentialsProvider = new DefaultAWSCredentialsProviderChain()
  val regionProvider = new DefaultAwsRegionProviderChain()

  // Copy objects must be done with the AWS S3Client
  val awsS3Client: AmazonS3 = AmazonS3ClientBuilder.standard().build()

  def s3Iterable(bucketName: String, initialStartAfter: Option[String]): scala.collection.immutable.Iterable[S3ObjectSummary] = new scala.collection.immutable.Iterable[S3ObjectSummary] {
    override def iterator = new Iterator[S3ObjectSummary] {
      var items: List[S3ObjectSummary] = List.empty
      var continuationToken: Option[String] = Some(null)
      var startAfter: Option[String] = initialStartAfter

      def getMore = {
        def getMoreWithRetry(retry: Int = 3): Try[ListObjectsV2Result] = {
          val lor = new ListObjectsV2Request()
          lor.setBucketName(bucketName)

          if (startAfter.nonEmpty) {
            lor.setStartAfter(startAfter.get)
            startAfter = None
          } else if (continuationToken.nonEmpty) {
            lor.setContinuationToken(continuationToken.get)
          }

          Try(awsS3Client.listObjectsV2(lor))
            .recoverWith {
              case _ if retry > 0 =>
                println(s"Retry S3 Get Items: #${3 - retry}")
                getMoreWithRetry(retry - 1)
              case ex => Failure(ex)
            }
        }

        getMoreWithRetry().foreach { res =>
          continuationToken = Option(res.getNextContinuationToken)
          items = items ++ res.getObjectSummaries.asScala.toList
        }
      }

      override def hasNext = {
        if (items.isEmpty && continuationToken.nonEmpty ) getMore

        items.nonEmpty
      }

      override def next() = {
        if (items.isEmpty && continuationToken.nonEmpty ) getMore

        val head = items.head
        val tail = items.tail
        items = tail

        head
      }
    }
  }

  def updateStorageClass(objectSummary: S3ObjectSummary): Future[(String, CopyObjectResult)] = {
    val srcBucket = objectSummary.getBucketName
    val srcKey = objectSummary.getKey
    val destBucket = srcBucket
    val destKey = srcKey
    val cor = new CopyObjectRequest(srcBucket, srcKey, destBucket, destKey)
    cor.setStorageClass(StorageClass.Standard)

    def performAction(retry: Int = 0): Future[(String, CopyObjectResult)] =
      Future {
        blocking {
          val res = awsS3Client.copyObject(cor)
          (destKey, res)
        }
      }.recoverWith[(String, CopyObjectResult)] {
        case _ if retry < 3 =>
          println(s"Retry: $retry for $srcKey")
          performAction(retry + 1)
        case ex =>
          println(s"failed after 3 retries for $srcKey")
          Future.failed(ex)
      }

    performAction()
  }

  if (args.length == 0) {
    println("please provide a name for the S3 bucket.")
    system.terminate()
  } else {
    val bucketName = args(0)
    val startAfter = Try { args(1) }.toOption

    Source(s3Iterable(bucketName, startAfter))
      .filter(_.getStorageClass == StorageClass.StandardInfrequentAccess.toString)
      .mapAsyncUnordered(25)(updateStorageClass)
      .withAttributes(supervisionStrategy(resumingDecider))
      .runForeach { case (key, res) => println(s"Updated storageClass on $key - ${res.getLastModifiedDate}") }
      .onComplete { tryRes =>
        tryRes match {
          case Success(_) => println("All Done!")
          case Failure(ex) => system.log.error(ex, "Failed :(")
        }
        system.terminate()
      }
  }
}
