import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.s3.impl.ListBucketVersion2
import akka.stream.alpakka.s3.scaladsl.{ListBucketResultContents, S3Client}
import akka.stream.alpakka.s3.{MemoryBufferType, S3Settings}
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.DefaultAwsRegionProviderChain
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import scala.concurrent.{Future, blocking}
import scala.language.postfixOps
import scala.util.{Failure, Success}

object Main extends App {
  implicit val system = ActorSystem("s3storageclass")
  implicit val ec = system.dispatcher
  implicit val mat = ActorMaterializer()

  val awsCredentialsProvider = new DefaultAWSCredentialsProviderChain()
  val regionProvider = new DefaultAwsRegionProviderChain()

  val settings = new S3Settings(
    MemoryBufferType,
    None,
    awsCredentialsProvider,
    regionProvider,
    true,
    None,
    ListBucketVersion2
  )

  // We will use a streaming source for the directory listing
  val s3Client: S3Client = new S3Client(settings)

  // Copy objects must be done with the AWS S3Client
  val awsS3Client: AmazonS3 = AmazonS3ClientBuilder.standard().build()

  def updateStorageClass(listBucketResult: ListBucketResultContents): Future[(String, CopyObjectResult)] = {
    val srcBucket = listBucketResult.bucketName
    val srcKey = listBucketResult.key
    val destBucket = srcBucket
    val destKey = srcKey

    val cor = new CopyObjectRequest(srcBucket, srcKey, destBucket, destKey)
    cor.setStorageClass(StorageClass.Standard)

    Future {
      blocking {
        val res = awsS3Client.copyObject(cor)
        (destKey, res)
      }
    }
  }

  if (args.length == 0) {
    println("please provide a name for the S3 bucket.")
    system.terminate()
  } else {
    val bucketName = args(0)

    s3Client.listBucket(bucketName, None)
      .filter(_.storageClass == StorageClass.StandardInfrequentAccess.toString)
      .mapAsyncUnordered(100)(updateStorageClass)
      .runForeach { case (key, res) => println(s"Updated storageClass on $key - ${res.getLastModifiedDate}") }
      .onComplete { tryRes =>
        tryRes match {
          case Success(res) => println("All Done!")
          case Failure(ex) => system.log.error(ex, "Failed :(")
        }
        system.terminate()
      }
  }
}
